from typing import Dict, Any
import redis
import time
import asyncio
import logging
from .consistent_hash import ConsistentHash

class VisitCounterService:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        # Redis sharding setup - connect to actual docker-compose services
        self.redis_nodes = {
            'redis_7070': redis.Redis(host='redis1', port=6379, decode_responses=True),
            'redis_7071': redis.Redis(host='redis2', port=6379, decode_responses=True)
        }
        
        # Initialize consistent hashing with virtual nodes
        self.consistent_hash = ConsistentHash(['redis_7070', 'redis_7071'])
        
        # Cache with node tracking
        self._cache: Dict[str, Dict[str, Any]] = {}
        self.cache_ttl = 5
        
        # Write buffer per shard
        self._write_buffer: Dict[str, Dict[str, int]] = {
            'redis_7070': {},
            'redis_7071': {}
        }
        self.flush_interval = 30
        self.is_buffer_flushing = False
        self._initialized = True
        self._last_flush_time = time.time()

    def _get_redis_client(self, page_id: str) -> tuple[redis.Redis, str]:
        """Get appropriate Redis client for a page_id with failover"""
        primary_node = self.consistent_hash.get_node(page_id)
        
        try:
            # Try primary node
            self.redis_nodes[primary_node].ping()
            return self.redis_nodes[primary_node], primary_node
        except (redis.ConnectionError, redis.TimeoutError):
            # Try failover to other node
            backup_node = 'redis_7071' if primary_node == 'redis_7070' else 'redis_7070'
            try:
                self.redis_nodes[backup_node].ping()
                return self.redis_nodes[backup_node], backup_node
            except (redis.ConnectionError, redis.TimeoutError):
                raise Exception("All Redis nodes are down")

    async def increment_visit(self, page_id: str) -> Dict[str, Any]:
        """Increment visit count with proper sharding"""
        try:
            # Check if buffer needs flushing
            current_time = time.time()
            if current_time - self._last_flush_time >= self.flush_interval:
                await self._flush_buffer()
                self._last_flush_time = current_time

            # Get Redis client with failover
            redis_client, node = self._get_redis_client(page_id)
            
            # Update write buffer
            if page_id not in self._write_buffer[node]:
                self._write_buffer[node][page_id] = 0
            self._write_buffer[node][page_id] += 1
            
            # Get current total
            try:
                redis_count = int(redis_client.get(f"page:{page_id}") or 0)
                buffer_count = self._write_buffer[node].get(page_id, 0)
                total_count = redis_count + buffer_count
                
                # Update cache
                self._cache[page_id] = {
                    'count': total_count,
                    'timestamp': current_time,
                    'node': node
                }
                
                return {
                    "visits": total_count,
                    "served_via": node
                }
            except Exception as e:
                logging.error(f"Redis error: {str(e)}")
                if page_id in self._cache:
                    return {
                        "visits": self._cache[page_id]['count'],
                        "served_via": "in_memory"
                    }
                return {"visits": 1, "served_via": "in_memory"}
                
        except Exception as e:
            logging.error(f"Error in increment_visit: {str(e)}")
            if page_id in self._cache:
                return {
                    "visits": self._cache[page_id]['count'],
                    "served_via": "in_memory"
                }
            return {"visits": 1, "served_via": "in_memory"}

    async def get_visit_count(self, page_id: str) -> Dict[str, Any]:
        """Get visit count with proper caching and failover"""
        try:
            current_time = time.time()
            
            # Check cache first
            cached_data = self._cache.get(page_id)
            if cached_data and (current_time - cached_data['timestamp']) < self.cache_ttl:
                return {
                    "visits": cached_data['count'],
                    "served_via": "in_memory"
                }
            
            # Get Redis client with failover
            try:
                redis_client, node = self._get_redis_client(page_id)
                
                # Get current total
                redis_count = int(redis_client.get(f"page:{page_id}") or 0)
                buffer_count = self._write_buffer[node].get(page_id, 0)
                total_count = redis_count + buffer_count
                
                # Update cache
                self._cache[page_id] = {
                    'count': total_count,
                    'timestamp': current_time,
                    'node': node
                }
                
                return {
                    "visits": total_count,
                    "served_via": node
                }
            except Exception:
                # If Redis is down but we have cached data
                if cached_data:
                    return {
                        "visits": cached_data['count'],
                        "served_via": "in_memory"
                    }
                return {"visits": 0, "served_via": "in_memory"}
                
        except Exception as e:
            logging.error(f"Error in get_visit_count: {str(e)}")
            return {"visits": 0, "served_via": "in_memory"}

    async def _flush_buffer(self) -> None:
        """Flush buffer to Redis with proper error handling"""
        if self.is_buffer_flushing:
            return

        try:
            self.is_buffer_flushing = True
            
            for node, buffer in self._write_buffer.items():
                if not buffer:
                    continue
                
                try:
                    redis_client = self.redis_nodes[node]
                    buffer_to_flush = buffer.copy()
                    buffer.clear()
                    
                    # Use pipeline for atomic updates
                    pipeline = redis_client.pipeline()
                    for page_id, count in buffer_to_flush.items():
                        if count > 0:
                            pipeline.incrby(f"page:{page_id}", count)
                    pipeline.execute()
                except Exception as e:
                    logging.error(f"Error flushing buffer to {node}: {str(e)}")
                    # Restore buffer
                    for page_id, count in buffer_to_flush.items():
                        if page_id not in buffer:
                            buffer[page_id] = 0
                        buffer[page_id] += count
                        
        except Exception as e:
            logging.error(f"Error in _flush_buffer: {str(e)}")
        finally:
            self.is_buffer_flushing = False
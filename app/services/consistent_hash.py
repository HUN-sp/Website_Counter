import hashlib
from typing import List, Dict, Any
from bisect import bisect

class ConsistentHash:
    def __init__(self, nodes: List[str], virtual_nodes: int = 100):
        """Initialize the consistent hash ring"""
        self.virtual_nodes = virtual_nodes
        self.hash_ring = {}  # Maps hash values to nodes
        self.sorted_keys = []  # Sorted list of hash values
        
        for node in nodes:
            self.add_node(node)
    
    def _get_hash(self, key: str) -> int:
        """Generate hash for a key"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_node(self, node: str) -> None:
        """Add a node to the hash ring"""
        for i in range(self.virtual_nodes):
            virtual_node = f"{node}_{i}"
            hash_value = self._get_hash(virtual_node)
            self.hash_ring[hash_value] = node
            self.sorted_keys.append(hash_value)
        self.sorted_keys.sort()
    
    def remove_node(self, node: str) -> None:
        """Remove a node from the hash ring"""
        for i in range(self.virtual_nodes):
            virtual_node = f"{node}_{i}"
            hash_value = self._get_hash(virtual_node)
            if hash_value in self.hash_ring:
                del self.hash_ring[hash_value]
                self.sorted_keys.remove(hash_value)
    
    def get_node(self, key: str) -> str:
        """Get the node responsible for the given key"""
        if not self.hash_ring:
            raise Exception("Hash ring is empty")
        
        key_hash = self._get_hash(key)
        index = bisect(self.sorted_keys, key_hash)
        
        # Wrap around to first node if past the end
        if index >= len(self.sorted_keys):
            index = 0
            
        return self.hash_ring[self.sorted_keys[index]]
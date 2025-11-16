from typing import Dict, Any, List, Optional, Union, get_origin, get_args
from pathlib import Path
import json

class SizeComputer:
    """Static methods to compute database, collection, and document sizes"""
    
    DEFAULT_SIZES = {
        "key_value_pair": 12,
        "number": 8,
        "string": 80,
        "date": 20,
        "long_string": 200
    }
    
    @staticmethod
    def load_key_sizes(key_sizes_path: str = None) -> Dict[str, int]:
        """
        Load key sizes from JSON file or use defaults
        
        Args:
            key_sizes_path: Path to key_sizes.json file
            
        Returns:
            Dictionary of type sizes
        """
        if key_sizes_path and Path(key_sizes_path).exists():
            with open(key_sizes_path, 'r') as f:
                return json.load(f)
        return SizeComputer.DEFAULT_SIZES.copy()
    
    @staticmethod
    def compute_field_size(
        field_type: type,
        field_name: str,
        key_sizes: Dict[str, int],
        field_specifics: Dict[str, Any] = None
    ) -> int:
        """
        Compute the size of a single field in bytes
        
        Args:
            field_type: Python type of the field
            field_name: Name of the field
            key_sizes: Dictionary of base type sizes
            field_specifics: Optional statistics (avg_length, null_percentage, etc.)
            
        Returns:
            Size in bytes
        """
        if field_specifics is None:
            field_specifics = {}
        
        avg_length = field_specifics.get("avg_length")
        null_percentage = field_specifics.get("null_percentage", 0)
        
        # Get the origin type
        origin = get_origin(field_type)
        
        # Handle Optional types (Optional[X] is Union[X, None])
        if origin is Union:
            args = get_args(field_type)
            # Noneyype
            non_none_types = [arg for arg in args if arg is not type(None)]
            if non_none_types:
                field_type = non_none_types[0]
                origin = get_origin(field_type)
        
        base_size = 0
        
        # List types
        if origin is list or origin is List:
            args = get_args(field_type)
            item_type = args[0] if args else str
            
            # Average number of items in the list
            avg_items = field_specifics.get("avg_items", 1)
            
            # Recursively compute item size
            item_size = SizeComputer.compute_field_size(
                item_type, 
                f"{field_name}_item",
                key_sizes,
                {}
            )
            
            # item_size * avg_items
            base_size = item_size * avg_items
        
        # primitive types
        elif field_type == int or field_type == float or field_type == bool:
            base_size = key_sizes.get("number", 8)
        
        elif field_type == str:
            if avg_length:
                base_size = avg_length
            else:
                base_size = key_sizes.get("string", 80)
        
        # Dict types
        elif origin is dict or origin is Dict:
            # Let's say it is like a long_string
            base_size = key_sizes.get("long_string", 200)
        
        # nested dataclasses
        elif hasattr(field_type, '__dataclass_fields__'):
            nested_specifics = field_specifics.get("nested_fields", {})
            base_size = SizeComputer.compute_dataclass_size(
                field_type,
                key_sizes,
                nested_specifics
            )
        
        else:
            # As default let's use the string size
            base_size = key_sizes.get("string", 80)
        
        # Add key-value pair overhead
        overhead = key_sizes.get("key_value_pair", 12)
        
        # field doesn't exist if null
        presence_factor = 1 - (null_percentage / 100)
        
        # final size
        total_size = (base_size + overhead) * presence_factor
        
        return int(total_size)
    
    @staticmethod
    def compute_dataclass_size(
        dataclass_type: type,
        key_sizes: Dict[str, int],
        field_specifics: Dict[str, Dict[str, Any]] = None
    ) -> int:
        """
        Compute the average size of a dataclass instance in bytes
        
        Args:
            dataclass_type: The dataclass type
            key_sizes: Dictionary of base type sizes
            field_specifics: Optional field-specific statistics
            
        Returns:
            Average document size in bytes
        """
        if field_specifics is None:
            field_specifics = {}
        
        if not hasattr(dataclass_type, '__dataclass_fields__'):
            raise ValueError(f"{dataclass_type} is not a dataclass")
        
        total_size = 0
        
        # Iterate through all fields
        for field_name, field_info in dataclass_type.__dataclass_fields__.items():
            field_type = field_info.type
            specifics = field_specifics.get(field_name, {})
            
            field_size = SizeComputer.compute_field_size(
                field_type,
                field_name,
                key_sizes,
                specifics
            )
            
            total_size += field_size
        
        return total_size
    
    @staticmethod
    def compute_collection_size(
        dataclass_type: type,
        document_count: int,
        key_sizes: Dict[str, int],
        field_specifics: Dict[str, Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Compute the total size of a collection
        
        Args:
            dataclass_type: The dataclass type representing documents
            document_count: Number of documents in the collection
            key_sizes: Dictionary of base type sizes
            field_specifics: Optional field-specific statistics
            
        Returns:
            Dictionary with size metrics
        """
        avg_doc_size = SizeComputer.compute_dataclass_size(
            dataclass_type,
            key_sizes,
            field_specifics
        )
        
        total_size_bytes = document_count * avg_doc_size
        
        return {
            "document_count": document_count,
            "avg_document_size_bytes": avg_doc_size,
            "total_size_bytes": total_size_bytes,
            "total_size_kb": total_size_bytes / 1024,
            "total_size_mb": total_size_bytes / (1024 * 1024),
            "total_size_gb": total_size_bytes / (1024 * 1024 * 1024)
        }
    
    @staticmethod
    def compute_database_size(
        collections: Dict[str, type],
        statistics: Dict[str, Any],
        key_sizes_path: str = None
    ) -> Dict[str, Any]:
        """
        Compute the total size of the database
        
        Args:
            collections: Dictionary of {collection_name: dataclass_type}
            statistics: Statistics dictionary from JSON file
            key_sizes_path: Optional path to key_sizes.json
            
        Returns:
            Dictionary with database-wide size metrics
        """
        key_sizes = SizeComputer.load_key_sizes(key_sizes_path)
        
        database_info = statistics.get("database", {})
        collections_stats = statistics.get("collections", {})
        
        collection_sizes = {}
        total_bytes = 0
        total_docs = 0
        
        for col_name, dataclass_type in collections.items():
            if col_name not in collections_stats:
                continue
            
            col_stats = collections_stats[col_name]
            document_count = col_stats.get("document_count", 0)
            field_specifics = col_stats.get("field_specifics", {})
            
            col_size = SizeComputer.compute_collection_size(
                dataclass_type,
                document_count,
                key_sizes,
                field_specifics
            )
            
            collection_sizes[col_name] = col_size
            total_bytes += col_size["total_size_bytes"]
            total_docs += col_size["document_count"]
        
        return {
            "database_name": database_info.get("name", "unknown"),
            "database_description": database_info.get("description", ""),
            "total_collections": len(collection_sizes),
            "total_documents": total_docs,
            "collections": collection_sizes,
            "total_size_bytes": total_bytes,
            "total_size_kb": total_bytes / 1024,
            "total_size_mb": total_bytes / (1024 * 1024),
            "total_size_gb": total_bytes / (1024 * 1024 * 1024),
            "total_size_tb": total_bytes / (1024 * 1024 * 1024 * 1024)
        }
    
    @staticmethod
    def format_size(bytes_value: float) -> str:
        """
        Format bytes to human readable string
        
        Args:
            bytes_value: Size in bytes
            
        Returns:
            Formatted string (e.g., "1.5 GB")
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.2f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.2f} PB"

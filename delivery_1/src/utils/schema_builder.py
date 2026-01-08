import json
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from dataclasses import make_dataclass, field
from enum import Enum

class SchemaBuilder:
    """Build Pythopn object from json schema"""
    
    def __init__(self, schema: Dict[str, Any]):
        self.schema: Dict[str, Any] = schema
    
    def create_dataclass_from_collection(self, collection_name: str):
        """
        Build a dataclass python from a json schema
        
        Args:
            collection_name: (ex: "Product", "Client")
        
        Returns:
            a data class corresponding to the collection
        """
        
        collection_def = self.schema.get("properties", {}).get(collection_name)
        if not collection_def:
            raise ValueError(f"Collection '{collection_name}' not found in schema")
        
        properties = collection_def.get("properties", {})
        required_fields = set(collection_def.get("required", []))
        
        required_fields_list = []
        optional_fields_list = []
        
        for field_name, field_def in properties.items():
            field_type = self._get_python_type(field_def, field_name)
            is_required = field_name in required_fields
            
            # Extract format for metadata
            json_format = field_def.get("format")
            metadata = {"format": json_format} if json_format else {}
            
            if not is_required:
                field_type = Optional[field_type]
                # Use field() to attach metadata and default value
                optional_fields_list.append((field_name, field_type, field(default=None, metadata=metadata)))
            else:
                # Use field() to attach metadata (default is MISSING)
                required_fields_list.append((field_name, field_type, field(metadata=metadata)))
        
        fields_list = required_fields_list + optional_fields_list
        
        new_class = make_dataclass(collection_name, fields_list)
        return new_class
    
    def _get_python_type(self, field_def: Dict[str, Any], field_name: str = ""):
        """
        Convert json schhema type in python
        Handle: primitives, arrays, objects, enums, etc.
        """
        json_type = field_def.get("type")
        json_format = field_def.get("format")
        
        # Case 1: simple cases like date, email ...
        if json_format in ["date", "date-time"]:
            return str
        elif json_format in ["email", "uri", "uuid"]:
            return str
        
        # Case 2: primitives
        if json_type == "integer":
            return int
        elif json_type == "number":
            return float
        elif json_type == "string":
            if "enum" in field_def:
                enum_values = field_def["enum"]
                enum_name = f"{field_name.capitalize()}Enum" if field_name else "ValueEnum"
                return Enum(enum_name, {val: val for val in enum_values})
            return str
        elif json_type == "boolean":
            return bool
        elif json_type == "null":
            return type(None)
        
        # Case 3: Array
        elif json_type == "array":
            items_def = field_def.get("items", {})
            item_type = self._get_python_type(items_def)
            return List[item_type]
        
        # Case 4: Object
        elif json_type == "object":
            if "properties" in field_def:
                nested_class_name = f"{field_name.capitalize()}Object" if field_name else "NestedObject"
                return self._create_nested_class(nested_class_name, field_def)
            return Dict[str, Any]
        
        # Case 6: Type multiple (ex: ["string", "null"])
        elif isinstance(json_type, list):
            types = [self._get_python_type({"type": t}) for t in json_type]
            return Union[tuple(types)]
        
        # Case 7: Reference $ref
        elif "$ref" in field_def:
            ref_path = field_def["$ref"]
            if ref_path.startswith("#/"):
                ref_name = ref_path.split("/")[-1]
                return self.create_dataclass_from_collection(ref_name)
            return Any
        return Any
    
    def _create_nested_class(self, class_name: str, object_def: Dict[str, Any]):
        """Build nested classes for "object into object" """
        
        properties = object_def.get("properties", {})
        required_fields = set(object_def.get("required", []))
        
        required_fields_list = []
        optional_fields_list = []

        for field_name, field_def in properties.items():
            field_type = self._get_python_type(field_def, field_name)
            is_required = field_name in required_fields
            
            # Extract format for metadata
            json_format = field_def.get("format")
            metadata = {"format": json_format} if json_format else {}
            
            if not is_required:
                field_type = Optional[field_type]
                optional_fields_list.append((field_name, field_type, field(default=None, metadata=metadata)))
            else:
                required_fields_list.append((field_name, field_type, field(metadata=metadata)))

        fields_list = required_fields_list + optional_fields_list
        
        new_class = make_dataclass(class_name, fields_list)
        return new_class
    
    def create_all_dataclasses(self) -> Dict[str, type]:
        """
        Build all the data classes for the collection
        
        Returns:
            Dict {collection name: dataclass}
        """
        
        dataclasses = {}
        
        for collection_name in self.schema.get("properties", {}).keys():
            dataclasses[collection_name] = self.create_dataclass_from_collection(collection_name)
        
        return dataclasses
    
    def get_schema_info(self) -> Dict[str, Any]:
        """Print details ion the schema"""
        
        return {
            "title": self.schema.get("title", "Unknown"),
            "schema_version": self.schema.get("$schema", "Unknown"),
            "collections": list(self.schema.get("properties", {}).keys()),
            "total_collections": len(self.schema.get("properties", {}))
        }
import json
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from dataclasses import make_dataclass
from enum import Enum
import random

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
        
        fields_list = []
        
        for field_name, field_def in properties.items():
            field_type = self._get_python_type(field_def, field_name)
            is_required = field_name in required_fields
            
            if not is_required:
                field_type = Optional[field_type]
                fields_list.append((field_name, field_type, None))
            else:
                fields_list.append((field_name, field_type))
        
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
        
        fields_list = []
        for field_name, field_def in properties.items():
            field_type = self._get_python_type(field_def, field_name)
            is_required = field_name in required_fields
            
            if not is_required:
                field_type = Optional[field_type]
                fields_list.append((field_name, field_type, None))
            else:
                fields_list.append((field_name, field_type))
        
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
    
    # def generate_docs_from_stats(collection_name: str, statistics: Dict[str, Any]) -> List[Dict[str, Any]]:
    #     """
    #     Génère des documents simulés pour une collection à partir de statistics JSON.
        
    #     Args:
    #         collection_name: nom de la collection (ex: "OrderLine")
    #         statistics: dictionnaire complet des stats JSON
        
    #     Returns:
    #         List[Dict] : liste de documents simulés
    #     """
    #     col_stats = statistics["collections"].get(collection_name)
    #     if not col_stats:
    #         raise ValueError(f"{collection_name} non trouvé dans les statistics")
        
    #     doc_count = col_stats.get("document_count", 0)
    #     field_stats = col_stats.get("field_specifics", {})

    #     docs = []
    #     for i in range(doc_count):
    #         doc = {}
    #         for field_name, specifics in field_stats.items():
    #             # générer int, float, str selon avg_length / occurrence_multiplier
    #             if "avg_length" in specifics:
    #                 length = specifics.get("avg_length", 10)
    #                 doc[field_name] = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=length))
    #             elif "occurrence_multiplier" in specifics:
    #                 doc[field_name] = i % (specifics["occurrence_multiplier"] * doc_count)
    #             else:
    #                 doc[field_name] = i  # simple incrément pour int / id
                
    #             # gérer null_percentage
    #             null_pct = specifics.get("null_percentage", 0)
    #             if random.random() < null_pct / 100:
    #                 doc[field_name] = None
    #         docs.append(doc)
    #     return docs
"""
Delivery 1 Service
Handles schema loading, object creation, examples, and size computation
"""

from pathlib import Path
from typing import Dict, Any
from utils.load_file import FileLoaderFactory
from utils.schema_builder import SchemaBuilder
from utils.size_computer import SizeComputer


class Delivery1Service:
    """Service for Delivery 1: Schema to Object and Size Computation"""
    
    def __init__(self, schema_path: str, stats_path: str, key_sizes_path: str):
        """
        Initialize the service with file paths
        
        Args:
            schema_path: Path to JSON schema file
            stats_path: Path to statistics JSON file
            key_sizes_path: Path to key sizes JSON file
        """
        self.schema_path = Path(schema_path)
        self.stats_path = Path(stats_path)
        self.key_sizes_path = Path(key_sizes_path)
        
        self.schema_data = None
        self.stats_data = None
        self.collections = None
        self.db_analysis = None
    
    def load_files(self):
        """Load all required files"""
        print("="*70)
        print("LOADING FILES")
        print("="*70)
        
        loader = FileLoaderFactory.registry['json']()
        
        print(f"\n   Loading schema: {self.schema_path.name}")
        self.schema_data = loader.load(str(self.schema_path))
        print(f"   Schema loaded successfully")
        
        print(f"\n   Loading statistics: {self.stats_path.name}")
        self.stats_data = loader.load(str(self.stats_path))
        print(f"   Statistics loaded successfully")
        
        print(f"\n   Using key sizes: {self.key_sizes_path.name}")
        print(f"   Key sizes file configured")
    
    def build_collections(self):
        """Build dataclasses from schema"""
        print("\n" + "="*70)
        print("BUILDING COLLECTIONS FROM SCHEMA")
        print("="*70)
        
        builder = SchemaBuilder(self.schema_data)
        
        schema_info = builder.get_schema_info()
        print(f"\n   Schema Title: {schema_info['title']}")
        print(f"   Schema Version: {schema_info['schema_version']}")
        print(f"   Total Collections: {schema_info['total_collections']}")
        
        self.collections = builder.create_all_dataclasses()
        print(f"\n   Created {len(self.collections)} dataclasses:")
        for class_name in self.collections.keys():
            fields_count = len(self.collections[class_name].__dataclass_fields__)
            print(f"      - {class_name} ({fields_count} fields)")
    
    def compute_sizes(self):
        """Compute sizes for all collections and database"""
        print("\n" + "="*70)
        print("COMPUTING DATABASE SIZES")
        print("="*70)
        
        # Compute database size
        self.db_analysis = SizeComputer.compute_database_size(
            collections=self.collections,
            statistics=self.stats_data,
            key_sizes_path=str(self.key_sizes_path)
        )
        
        # Display database overview
        print(f"\n   Database: {self.db_analysis['database_name']}")
        print(f"   Description: {self.db_analysis['database_description']}")
        print(f"   Total Collections: {self.db_analysis['total_collections']}")
        print(f"   Total Documents: {self.db_analysis['total_documents']:,}")
        print(f"   Total Size: {SizeComputer.format_size(self.db_analysis['total_size_bytes'])}")
        print(f"              ({self.db_analysis['total_size_gb']:.2f} GB)")
    
    def display_size_breakdown(self):
        """Display detailed size breakdown for each collection"""
        print("\n" + "="*70)
        print("COLLECTION SIZE BREAKDOWN")
        print("="*70)
        
        # Sort collections by size
        sorted_collections = sorted(
            self.db_analysis['collections'].items(),
            key=lambda x: x[1]['total_size_bytes'],
            reverse=True
        )
        
        for col_name, col_size in sorted_collections:
            percentage = (col_size['total_size_bytes'] / self.db_analysis['total_size_bytes']) * 100
            
            print(f"\n    {col_name}")
            print(f"      Documents: {col_size['document_count']:,}")
            print(f"      Avg Doc Size: {col_size['avg_document_size_bytes']:,} bytes")
            print(f"      Total Size: {SizeComputer.format_size(col_size['total_size_bytes'])}")
            print(f"      Percentage: {percentage:.2f}%")
            
            # Visual bar
            bar_length = min(50, int(percentage / 2))
            bar = "█" * bar_length
            print(f"      [{bar}]")
    
    def display_summary(self):
        """Display final summary"""
        print("\n" + "="*70)
        print(" SUMMARY")
        print("="*70)
        
        print(f"\n    Successfully created {len(self.collections)} dataclasses from JSON Schema")
        print(f"    Computed size for {self.db_analysis['total_collections']} collections")
        print(f"    Total database size: {SizeComputer.format_size(self.db_analysis['total_size_bytes'])}")
        
        # Top 3 largest collections
        sorted_collections = sorted(
            self.db_analysis['collections'].items(),
            key=lambda x: x[1]['total_size_bytes'],
            reverse=True
        )
        
        print(f"\n    Top 3 largest collections:")
        for i, (col_name, col_size) in enumerate(sorted_collections[:3], 1):
            print(f"      {i}. {col_name}: {SizeComputer.format_size(col_size['total_size_bytes'])}")
    
    def run(self):
        """Execute the complete delivery 1 workflow"""
        try:
            self.load_files()
            self.build_collections()
            self.compute_sizes()
            self.display_size_breakdown()
            self.display_summary()
            
            return True
        
        except Exception as e:
            print(f"\nError during execution: {e}")
            import traceback
            traceback.print_exc()
            return False

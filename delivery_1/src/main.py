#!/usr/bin/env python3
"""
Main entry point for Big Data Structure project
Supports different delivery commands with configurable file paths
"""

import argparse
import sys
from pathlib import Path
from services.delivery_1_service import Delivery1Service


def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(
        description='Big Data Structure - Database Schema and Size Analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run delivery 1 with default files
  python main.py --delivery_1
  
  # Run delivery 1 with custom files
  python main.py --delivery_1 --schema custom_schema.json --stats custom_stats.json --sizes custom_sizes.json
  
  # Show version
  python main.py --version
        """
    )
    
    # Version
    parser.add_argument(
        '--version',
        action='version',
        version='Big Data Structure v1.0.0'
    )
    
    # Delivery commands
    parser.add_argument(
        '--delivery_1',
        action='store_true',
        help='Run Delivery 1: Schema to Object and Size Computation'
    )
    
    # File paths
    parser.add_argument(
        '--schema',
        type=str,
        default='basic_schema.json',
        help='Path to JSON schema file (default: basic_schema.json)'
    )
    
    parser.add_argument(
        '--stats',
        type=str,
        default='basic_statistic.json',
        help='Path to statistics JSON file (default: basic_statistic.json)'
    )
    
    parser.add_argument(
        '--sizes',
        type=str,
        default='key_sizes.json',
        help='Path to key sizes JSON file (default: key_sizes.json)'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Check if at least one delivery command is specified
    if not any([args.delivery_1]):
        parser.print_help()
        print("\nError: Please specify at least one delivery command (e.g., --delivery_1)")
        sys.exit(1)
    
    # Resolve file paths relative to project root
    project_root = Path(__file__).parent.parent
    
    schema_path = project_root / args.schema
    stats_path = project_root / args.stats
    sizes_path = project_root / args.sizes
    
    # Validate files exist
    missing_files = []
    if not schema_path.exists():
        missing_files.append(f"Schema file not found: {schema_path}")
    if not stats_path.exists():
        missing_files.append(f"Statistics file not found: {stats_path}")
    if not sizes_path.exists():
        missing_files.append(f"Key sizes file not found: {sizes_path}")
    
    if missing_files:
        print("Error: Missing required files:")
        for error in missing_files:
            print(f"  - {error}")
        sys.exit(1)
    
    # Execute delivery commands
    success = True
    
    if args.delivery_1:
        print("\n" + "="*70)
        print(" DELIVERY 1: SCHEMA TO OBJECT AND SIZE COMPUTATION")
        print("="*70)
        print(f"\nConfiguration:")
        print(f"  Schema: {schema_path.name}")
        print(f"  Statistics: {stats_path.name}")
        print(f"  Key Sizes: {sizes_path.name}")
        
        # Create and run the service
        service = Delivery1Service(
            schema_path=str(schema_path),
            stats_path=str(stats_path),
            key_sizes_path=str(sizes_path)
        )
        
        success = service.run()
    
    # Exit with appropriate code
    if success:
        print("\n" + "="*70)
        print(" EXECUTION COMPLETED SUCCESSFULLY")
        print("="*70)
        sys.exit(0)
    else:
        print("\n" + "="*70)
        print(" EXECUTION FAILED")
        print("="*70)
        sys.exit(1)


if __name__ == "__main__":
    main()

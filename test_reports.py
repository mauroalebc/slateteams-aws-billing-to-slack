#!/usr/bin/env python3

import os
import sys
import json

# Add current directory to path to import handler
sys.path.append('.')
from handler import report_cost

def test_report_types():
    """Test different report types with example data"""
    
    # Load example data
    with open("example_boto3_result.json", "r") as f:
        example_result = json.load(f)
    
    print("=" * 60)
    print("🔍 DEMONSTRAÇÃO DOS TIPOS DE RELATÓRIO")
    print("=" * 60)
    
    # Test 1: Daily (original)
    print("\n📅 1. RELATÓRIO DIÁRIO (padrão - apenas ontem)")
    print("-" * 50)
    summary, buffer, _ = report_cost(
        group_by="SERVICE", 
        length=5, 
        cost_aggregation="UnblendedCost", 
        result=example_result, 
        yesterday="2021-08-23", 
        new_method=True,
        report_type="daily"
    )
    print(f"Summary: {summary}")
    print(f"\nDetalhes:\n{buffer}")
    
    # Test 2: Total
    print("\n💰 2. RELATÓRIO TOTAL (soma do período)")
    print("-" * 50)
    summary, buffer, _ = report_cost(
        group_by="SERVICE", 
        length=5, 
        cost_aggregation="UnblendedCost", 
        result=example_result, 
        yesterday="2021-08-23", 
        new_method=True,
        report_type="total"
    )
    print(f"Summary: {summary}")
    print(f"\nDetalhes:\n{buffer}")
    
    # Test 3: Average  
    print("\n📊 3. RELATÓRIO MÉDIO (média diária do período)")
    print("-" * 50)
    summary, buffer, _ = report_cost(
        group_by="SERVICE", 
        length=5, 
        cost_aggregation="UnblendedCost", 
        result=example_result, 
        yesterday="2021-08-23", 
        new_method=True,
        report_type="average"
    )
    print(f"Summary: {summary}")
    print(f"\nDetalhes:\n{buffer}")
    
    print("\n" + "=" * 60)
    print("✅ Agora você pode escolher o tipo de relatório que prefere!")
    print("=" * 60)

if __name__ == "__main__":
    # Set n_days to simulate period
    os.environ["N_DAYS"] = "7"  # Using 7 days for demo
    test_report_types() 
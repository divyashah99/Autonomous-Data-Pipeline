"""
Main Entry Point for Autonomous Data Pipeline
Uses Google ADK (Vertex AI + Gemini) for intelligent agent orchestration
"""

import logging
import os
from agents_adk import IngestionAgent, QualityAgent, TransformAgent, LoaderAgent
from orchestrator_adk import PipelineManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# ============================================================================
# GCP CONFIGURATION
# ============================================================================
PROJECT_ID = "autonomousdatapipeline"
BUCKET_NAME = "pipeline-input-divya"
DATASET_ID = "pipeline_results"
LOCATION = "us-central1"  # Vertex AI location

# ============================================================================
# PIPELINE CONFIGURATION
# ============================================================================
# Set to True to use LLM for routing decisions (more intelligent but can be unpredictable)
# Set to False to use rule-based routing (more reliable, follows strict thresholds)
USE_LLM_ROUTING = False  # Recommended: False for reliability

print("""
╔════════════════════════════════════════════════════════════════════╗
║                                                                    ║
║     🤖 AUTONOMOUS DATA PIPELINE - GOOGLE ADK                       ║
║                                                                    ║
║     Powered by: Vertex AI Reasoning Engine + Gemini                ║
║                                                                    ║
╚════════════════════════════════════════════════════════════════════╝
""")

def main():
    """Main execution function"""
    
    print(f"\n🔧 Initializing Agents with Vertex AI...")
    print(f"   Project: {PROJECT_ID}")
    print(f"   Location: {LOCATION}")
    print(f"   Bucket: {BUCKET_NAME}")
    print(f"   Dataset: {DATASET_ID}\n")
    
    # ========================================
    # Initialize ADK Agents
    # ========================================
    print("📦 Creating Agent Instances...")
    
    ingest = IngestionAgent(
        project_id=PROJECT_ID,
        bucket_name=BUCKET_NAME,
        location=LOCATION
    )
    print("  ✓ Ingestion Agent (Gemini-powered)")
    
    quality = QualityAgent(
        project_id=PROJECT_ID,
        location=LOCATION
    )
    print("  ✓ Quality Agent (Gemini-powered)")
    
    transform = TransformAgent(
        project_id=PROJECT_ID,
        location=LOCATION
    )
    print("  ✓ Transform Agent (Gemini-powered)")
    
    loader = LoaderAgent(
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        location=LOCATION
    )
    print("  ✓ Loader Agent (Gemini-powered)")
    
    # ========================================
    # Initialize Pipeline Manager
    # ========================================
    manager = PipelineManager(
        ingestion=ingest,
        quality=quality,
        transform=transform,
        loader=loader,
        project_id=PROJECT_ID,
        location=LOCATION
    )
    print("  ✓ Pipeline Manager (Gemini orchestration)")
    
    print("\n" + "="*70)
    print("🚀 PIPELINE READY")
    print("="*70 + "\n")
    
    # ========================================
    # Test Files
    # ========================================
    files = [
        'day1_clean.csv',      # High quality - should proceed directly
        'day2_messy.csv',      # Medium quality - should trigger cleaning
        'day3_schema_change.csv'  # Schema change - should adapt
    ]
    
    # ========================================
    # Process Each File
    # ========================================
    results = []
    
    routing_mode = "LLM-powered (intelligent)" if USE_LLM_ROUTING else "Rule-based (reliable)"
    print(f"⚙️  Routing Mode: {routing_mode}\n")
    
    for file_name in files:
        try:
            result = manager.process_file(file_name, use_llm_routing=USE_LLM_ROUTING)
            results.append(result)
            
        except Exception as e:
            logging.error(f"❌ Pipeline execution failed for {file_name}: {e}")
            results.append({
                "file": file_name,
                "status": "FAILED",
                "error": str(e)
            })
    
    # ========================================
    # Summary Report
    # ========================================
    print("\n" + "="*70)
    print("📊 PIPELINE EXECUTION SUMMARY")
    print("="*70 + "\n")
    
    success_count = sum(1 for r in results if r['status'] == 'SUCCESS')
    failed_count = sum(1 for r in results if r['status'] == 'FAILED')
    aborted_count = sum(1 for r in results if r['status'] == 'ABORTED')
    
    print(f"Total Files Processed: {len(files)}")
    print(f"  ✅ Successful: {success_count}")
    print(f"  ❌ Failed: {failed_count}")
    print(f"  ⛔ Aborted: {aborted_count}")
    
    print("\nDetailed Results:")
    print("-" * 70)
    
    for result in results:
        status_icon = {
            'SUCCESS': '✅',
            'FAILED': '❌',
            'ABORTED': '⛔'
        }.get(result['status'], '❓')
        
        print(f"\n{status_icon} {result['file']}")
        print(f"   Status: {result['status']}")
        
        if result['status'] == 'SUCCESS':
            print(f"   Quality Score: {result.get('quality_score', 'N/A')}/100")
            print(f"   Rows Loaded: {result.get('rows_loaded', 0)}")
            print(f"   Transformed: {'Yes' if result.get('transformation_applied') else 'No'}")
            if result.get('schema_updated'):
                print(f"   Schema Updated: Yes (new columns: {result.get('new_columns')})")
        
        elif result['status'] == 'FAILED':
            print(f"   Error: {result.get('error', 'Unknown')}")
        
        elif result['status'] == 'ABORTED':
            print(f"   Reason: {result.get('reason', 'Quality too low')}")
            print(f"   Issues: {len(result.get('issues', []))} detected")
    
    print("\n" + "="*70)
    print("🎉 PIPELINE EXECUTION COMPLETE")
    print("="*70 + "\n")


if __name__ == "__main__":
    # Check for required environment variables
    required_env_vars = ['GOOGLE_APPLICATION_CREDENTIALS']
    missing_vars = [var for var in required_env_vars if var not in os.environ]
    
    if missing_vars:
        print(f"⚠️  WARNING: Missing environment variables: {missing_vars}")
        print("   Make sure to set GOOGLE_APPLICATION_CREDENTIALS")
        print("   Example: export GOOGLE_APPLICATION_CREDENTIALS='/path/to/service-account-key.json'")
        print()
    
    main()
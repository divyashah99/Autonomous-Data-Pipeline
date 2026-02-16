"""
Pipeline Orchestrator using Vertex AI Reasoning Engine
Coordinates multi-agent workflow with intelligent decision-making
"""

import time
import logging
import json
from typing import Dict, Any
import vertexai
from vertexai.generative_models import GenerativeModel


class PipelineManager:
    """
    Orchestrates the entire pipeline with LLM-powered decision making
    Uses Gemini to make intelligent routing and retry decisions
    """
    
    def __init__(self, ingestion, quality, transform, loader, project_id: str, location: str = "us-central1"):
        self.ingestion = ingestion
        self.quality = quality
        self.transform = transform
        self.loader = loader
        
        # Initialize Gemini for orchestration decisions
        vertexai.init(project=project_id, location=location)
        # Use gemini-pro which is widely available
        self.orchestrator_llm = GenerativeModel("gemini-2.5-flash")
        
        logging.info("🚀 Pipeline Manager initialized with Gemini orchestration")
    
    def safe_run(self, agent_func, *args, retries=2, agent_name="Agent"):
        """
        Execute agent with retry logic and LLM-powered failure analysis
        """
        for i in range(retries + 1):
            try:
                result = agent_func(*args)
                return result
                
            except Exception as e:
                error_context = f"""
Agent: {agent_name}
Attempt: {i+1}/{retries+1}
Error: {str(e)}
Error Type: {type(e).__name__}
Arguments: {args}
"""
                
                if i < retries:
                    # Ask Gemini if retry makes sense
                    retry_question = f"""
An agent failed with the following error. Should we retry or abort?

{error_context}

Consider:
1. Is this a transient error (network, timeout) or permanent (invalid data, auth)?
2. Will retrying likely succeed?
3. What's the risk of retry vs abort?

Respond with: RETRY or ABORT, followed by brief reasoning.
"""
                    
                    retry_decision = self.orchestrator_llm.generate_content(retry_question).text
                    logging.warning(f"🧠 Gemini retry analysis: {retry_decision}")
                    
                    if "ABORT" in retry_decision.upper():
                        logging.error(f"❌ Gemini recommends aborting after failure")
                        raise
                    
                    logging.warning(f"⚠️  Retry {i+1}/{retries} after error: {e}")
                    time.sleep(1)
                else:
                    logging.error(f"❌ Final failure in {agent_name}. Alerting Admin.")
                    raise
    
    def make_routing_decision(self, quality_score: int, issues: list, use_llm: bool = True) -> str:
        """
        Use Gemini to make intelligent pipeline routing decisions
        Can fall back to rule-based logic if use_llm=False
        """
        # For very clear cases, use rule-based logic directly
        if not use_llm or (quality_score == 100 and len(issues) == 0):
            if quality_score < 60:
                logging.info(f"📊 Rule-based decision: ABORT (score {quality_score} < 60)")
                return "ABORT"
            elif 60 <= quality_score <= 80:
                logging.info(f"📊 Rule-based decision: CLEAN (score {quality_score} in 60-80)")
                return "CLEAN"
            else:
                logging.info(f"📊 Rule-based decision: PROCEED (score {quality_score} > 80)")
                return "PROCEED"
        
        # Use LLM for nuanced decisions
        decision_context = f"""
Quality Score: {quality_score}/100
Issues Detected: {json.dumps(issues, indent=2)}

Standard Rules:
- Score < 60: ABORT pipeline
- Score 60-80: CLEAN data and proceed
- Score > 80: PROCEED directly (skip cleaning)

Your task: Analyze this specific case and decide: ABORT, CLEAN, or PROCEED.
Consider the types of issues, their severity, and business impact.

IMPORTANT: Start your response with "Decision: [ABORT|CLEAN|PROCEED]" on the first line.
"""
        
        response = self.orchestrator_llm.generate_content(decision_context)
        decision_text = response.text
        
        logging.info(f"🧠 Gemini routing decision: {decision_text[:200]}")
        
        # Enhanced parsing logic - check first 3 lines for clear decision
        first_lines = decision_text[:500].upper()
        
        # Priority-based parsing: Look for explicit decision markers first
        if "DECISION: PROCEED" in first_lines or "### **DECISION: PROCEED" in first_lines:
            return "PROCEED"
        elif "DECISION: CLEAN" in first_lines or "### **DECISION: CLEAN" in first_lines:
            return "CLEAN"
        elif "DECISION: ABORT" in first_lines or "### **DECISION: ABORT" in first_lines:
            return "ABORT"
        
        # Fallback: Use rule-based logic with quality score
        if quality_score < 60:
            logging.info(f"📊 Fallback: Using rule-based ABORT (score {quality_score} < 60)")
            return "ABORT"
        elif 60 <= quality_score <= 80:
            logging.info(f"📊 Fallback: Using rule-based CLEAN (score {quality_score} in 60-80)")
            return "CLEAN"
        else:
            logging.info(f"📊 Fallback: Using rule-based PROCEED (score {quality_score} > 80)")
            return "PROCEED"
    
    def process_file(self, file_name: str, use_llm_routing: bool = False) -> Dict[str, Any]:
        """
        Main pipeline orchestration with LLM-powered decision making
        
        Args:
            file_name: Name of the file to process
            use_llm_routing: If True, use LLM for routing decisions. If False, use rule-based logic.
        """
        print(f"\n{'='*70}")
        print(f"🚀 AUTONOMOUS PIPELINE: {file_name}")
        print(f"{'='*70}\n")
        
        try:
            # ========================================
            # STEP 1: INGESTION
            # ========================================
            print("📥 STEP 1: Ingestion Agent")
            print("-" * 70)
            
            ingest_out = self.safe_run(
                self.ingestion.execute, 
                file_name,
                agent_name="Ingestion Agent"
            )
            
            data = ingest_out['data']
            metadata = ingest_out['metadata']
            
            print(f"✅ Ingested {metadata['rows']} rows")
            print(f"   Format: {metadata['format']}")
            print(f"   Schema: {metadata['schema']}")
            
            if metadata['schema_changed']:
                print(f"⚠️  SCHEMA CHANGE DETECTED!")
                print(f"   New columns: {metadata['new_columns']}")
                print(f"   LLM Analysis: {metadata['llm_analysis'][:150]}...")
            
            print()
            
            # ========================================
            # STEP 2: QUALITY ASSESSMENT
            # ========================================
            print("🔍 STEP 2: Quality Agent")
            print("-" * 70)
            
            quality_out = self.safe_run(
                self.quality.execute,
                data,
                agent_name="Quality Agent"
            )
            
            score = quality_out['quality_score']
            issues = quality_out['issues']
            
            print(f"📊 Quality Score: {score}/100")
            print(f"   Issues found: {len(issues)}")
            for issue in issues[:3]:  # Show first 3 issues
                print(f"   - {issue['type']}: {issue.get('column', 'N/A')} ({issue.get('count', 0)} affected)")
            
            print()
            
            # ========================================
            # STEP 3: INTELLIGENT ROUTING DECISION
            # ========================================
            mode = "Rule-Based" if not use_llm_routing else "Gemini-Powered"
            print(f"🤔 STEP 3: Routing Decision ({mode})")
            print("-" * 70)
            
            decision = self.make_routing_decision(score, issues, use_llm=use_llm_routing)
            
            print(f"🎯 Decision: {decision}")
            
            if decision == "ABORT":
                print(f"❌ ABORTING: Quality score {score} too low")
                print(f"   Recommendation: Fix data at source and resubmit")
                return {
                    "file": file_name,
                    "status": "ABORTED",
                    "score": score,
                    "reason": f"Quality score {score} below threshold",
                    "issues": issues
                }
            
            print()
            
            # ========================================
            # STEP 4: TRANSFORMATION (if needed)
            # ========================================
            if decision == "CLEAN":
                print("🧹 STEP 4: Transform Agent")
                print("-" * 70)
                print(f"⚙️  Cleaning mediocre quality data (score: {score})...")
                
                trans_out = self.safe_run(
                    self.transform.execute,
                    data,
                    issues,
                    agent_name="Transform Agent"
                )
                
                data = trans_out['data']
                report = trans_out['report']
                
                print(f"✅ Transformation complete")
                print(f"   Rows: {report['rows_in']} → {report['rows_out']} ({report['rows_removed']} removed)")
                print(f"   Efficiency: {report['cleaning_efficiency']}%")
                print(f"   Fixes applied:")
                for fix in report['fixes_applied']:
                    print(f"   - {fix}")
                
                print()
            else:
                print("✨ STEP 4: Transform Agent")
                print("-" * 70)
                print(f"⏭️  Skipping transformation (high quality score: {score})")
                print()
            
            # ========================================
            # STEP 5: LOADING
            # ========================================
            print("📤 STEP 5: Loader Agent")
            print("-" * 70)
            
            loader_out = self.safe_run(
                self.loader.execute,
                data,
                metadata,
                agent_name="Loader Agent"
            )
            
            if loader_out['status'] == 'success':
                print(f"✅ Successfully loaded to {loader_out['destination']}")
                print(f"   Rows loaded: {loader_out['rows_loaded']}")
            else:
                print(f"❌ Load failed: {loader_out.get('error', 'Unknown error')}")
                raise Exception(f"Load failed: {loader_out.get('error')}")
            
            print()
            
            # ========================================
            # FINAL REPORT
            # ========================================
            print("=" * 70)
            print("📋 FINAL PIPELINE REPORT")
            print("=" * 70)
            
            final_report = {
                "file": file_name,
                "status": "SUCCESS",
                "quality_score": score,
                "issues_detected": len(issues),
                "transformation_applied": decision == "CLEAN",
                "rows_loaded": loader_out['rows_loaded'],
                "schema_updated": metadata['schema_changed'],
                "new_columns": metadata['new_columns'] if metadata['schema_changed'] else [],
                "destination": loader_out['destination']
            }
            
            print(f"✅ Status: {final_report['status']}")
            print(f"   File: {final_report['file']}")
            print(f"   Quality Score: {final_report['quality_score']}/100")
            print(f"   Rows Loaded: {final_report['rows_loaded']}")
            print(f"   Transformation: {'Yes' if final_report['transformation_applied'] else 'No'}")
            print(f"   Schema Updated: {'Yes' if final_report['schema_updated'] else 'No'}")
            
            if final_report['new_columns']:
                print(f"   New Columns: {final_report['new_columns']}")
            
            print("=" * 70)
            print()
            
            return final_report
            
        except Exception as e:
            logging.error(f"💥 Pipeline failed for {file_name}: {e}")
            print(f"\n❌ PIPELINE FAILED: {e}\n")
            
            return {
                "file": file_name,
                "status": "FAILED",
                "error": str(e)
            }
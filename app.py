from orchestrator import orchestrator_agent
from query_agent import DatabaseInfo
from pathlib import Path

init_db_info = DatabaseInfo(csv_path=Path(), db_schema={})
app = orchestrator_agent.to_web(deps=init_db_info)
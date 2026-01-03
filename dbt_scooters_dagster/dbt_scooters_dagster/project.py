from pathlib import Path

from dagster_dbt import DbtProject

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent.resolve()

if (DBT_PROJECT_DIR / "profiles.yml").exists():
    profiles_dir = DBT_PROJECT_DIR
else:
    profiles_dir = Path.home() / ".dbt"

dbt_scooters_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=profiles_dir,
    packaged_project_dir=DBT_PROJECT_DIR,
)
dbt_scooters_project.prepare_if_dev()

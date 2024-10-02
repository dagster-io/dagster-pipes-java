from dagster import (
    AssetExecutionContext,
    Definitions,
    MaterializeResult,
    PipesSubprocessClient,
    asset,
)

from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent

JAVA_SCRIPT_PATH = ROOT_DIR / "script.java"


@asset(description="An asset which is computed using an external Java script")
def java_asset(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
) -> MaterializeResult:
    return pipes_subprocess_client.run(
        context=context, command=["java", str(JAVA_SCRIPT_PATH)]
    ).get_materialize_result()


defs = Definitions(
    assets=[java_asset],
    resources={"pipes_subprocess_client": PipesSubprocessClient()},
)

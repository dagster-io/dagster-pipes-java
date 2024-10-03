from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    PipesSubprocessClient,
    asset,
    materialize,
)
from pathlib import Path
import subprocess
import pytest

ROOT_DIR = Path(__file__).parent.parent

CLASS_PATH = ROOT_DIR / "build/classes/java/main/pipes/PipesMappingParamsLoader.class"


@pytest.fixture(scope="session")
def built_jar() -> bool:
    subprocess.run(["./gradlew", "build"], check=True)
    return True


def test_java_pipes(built_jar: bool):
    @asset
    def java_asset(
        context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
    ) -> MaterializeResult:
        return pipes_subprocess_client.run(
            context=context,
            command=[
                "java",
                "-jar",
                str(ROOT_DIR / "build/libs/dagster-pipes-java-1.0-SNAPSHOT.jar"),
            ],
            extras={"input": "Hello, world!"},
        ).get_materialize_result()

    materialize(
        [java_asset], resources={"pipes_subprocess_client": PipesSubprocessClient()}
    )

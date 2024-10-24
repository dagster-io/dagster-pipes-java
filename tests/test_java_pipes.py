from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    PipesSubprocessClient,
    asset,
    materialize,
)
from typing import Dict, Any

from pathlib import Path
import subprocess
import pytest
from pytest_cases import parametrize
from dagster._core.pipes.utils import (
    PipesEnvContextInjector,
    PipesTempFileContextInjector,
)
from dagster._core.pipes.client import PipesContextInjector
import json
from hypothesis_jsonschema import from_schema

extras_strategy = from_schema({"type": ["object"]})


ROOT_DIR = Path(__file__).parent.parent

CLASS_PATH = ROOT_DIR / "build/classes/java/main/pipes/PipesMappingParamsLoader.class"


@pytest.fixture(scope="session", autouse=True)
def built_jar():
    subprocess.run(["./gradlew", "build"], check=True)


EXTRAS_LIST = [
    {
        "foo": "bar",
    },
    {
        "foo": "bar",
        "baz": 1,
    },
    {
        "foo": "bar",
        "baz": 1,
        "qux": [1, 2, 3],
    },
    {
        "foo": "bar",
        "baz": 1,
        "qux": [1, 2, 3],
        "quux": {"a": 1, "b": 2},
    },
    {
        "foo": "bar",
        "baz": 1,
        "qux": [1, 2, 3],
        "quux": {"a": 1, "b": 2},
        "corge": None,
    },
]


CUSTOM_MESSAGE_PAYLOADS = EXTRAS_LIST.copy() + [
    1,
    1.0,
    "foo",
    [1, 2, 3],
]


@parametrize("extras", EXTRAS_LIST)
@parametrize(
    "context_injector", [PipesEnvContextInjector(), PipesTempFileContextInjector()]
)
def test_java_pipes_components(
    context_injector: PipesContextInjector,
    extras: Dict[str, Any],
    tmpdir_factory,
    capsys,
):
    work_dir = tmpdir_factory.mktemp("work_dir")

    extras_path = work_dir / "extras.json"

    with open(str(extras_path), "w") as f:
        json.dump(extras, f)

    @asset
    def java_asset(
        context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
    ) -> MaterializeResult:
        job_name = context.dagster_run.job_name

        args = [
            "java",
            "-jar",
            str(ROOT_DIR / "build/libs/dagster-pipes-java-1.0-SNAPSHOT.jar"),
            "--env",
            f"--extras={str(extras_path)}",
            f"--jobName={job_name}",
        ]

        return pipes_subprocess_client.run(
            context=context,
            command=args,
            extras=extras,
        ).get_materialize_result()

    result = materialize(
        [java_asset],
        resources={
            "pipes_subprocess_client": PipesSubprocessClient(
                context_injector=context_injector
            )
        },
        raise_on_error=False,
    )

    assert result.success


@parametrize("extras", EXTRAS_LIST)
@parametrize("custom_message_payload", CUSTOM_MESSAGE_PAYLOADS)
@parametrize(
    "context_injector", [PipesEnvContextInjector(), PipesTempFileContextInjector()]
)
def test_java_pipes(
    context_injector: PipesContextInjector,
    extras: Dict[str, Any],
    custom_message_payload: Any,
    tmpdir_factory,
    capsys,
):
    work_dir = tmpdir_factory.mktemp("work_dir")

    extras_path = work_dir / "extras.json"
    custom_payload_path = work_dir / "custom_payload.json"

    with open(str(extras_path), "w") as f:
        json.dump(extras, f)

    with open(str(custom_payload_path), "w") as f:
        json.dump({"payload": custom_message_payload}, f)

    @asset
    def java_asset(
        context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
    ) -> MaterializeResult:
        job_name = context.dagster_run.job_name

        args = [
            "java",
            "-jar",
            str(ROOT_DIR / "build/libs/dagster-pipes-java-1.0-SNAPSHOT.jar"),
            "--env",
            f"--extras={str(extras_path)}",
            f"--jobName={job_name}",
            "--custom-payload-path",
            str(custom_payload_path),
            "--full",
        ]

        invocation_result = pipes_subprocess_client.run(
            context=context,
            command=args,
            extras=extras,
        )

        assert invocation_result.get_custom_messages()[0] == custom_message_payload

        materialization = invocation_result.get_materialize_result()

        return materialization

    result = materialize(
        [java_asset],
        resources={
            "pipes_subprocess_client": PipesSubprocessClient(
                context_injector=context_injector
            )
        },
        raise_on_error=False,
    )

    assert result.success

    captured = capsys.readouterr()

    assert (
        "[pipes] did not receive any messages from external process" not in captured.err
    )

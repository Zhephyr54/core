"""Tests for the Overkiz command executor."""

import asyncio
from collections.abc import Generator
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from freezegun.api import FrozenDateTimeFactory
from pyoverkiz.enums import APIType, ExecutionState, OverkizCommand
from pyoverkiz.exceptions import TooManyConcurrentRequestsError
from pyoverkiz.models import Action, Command
import pytest

from homeassistant.components.cover import (
    DOMAIN as COVER_DOMAIN,
    SERVICE_OPEN_COVER,
    CoverState,
)
from homeassistant.components.overkiz.executor import OverkizExecutor
from homeassistant.const import ATTR_ENTITY_ID, Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError

from .conftest import MockOverkizClient, SetupOverkizIntegration
from .helpers import async_deliver_events, execution_state_changed_event


LOCAL_COVER_DEVICE_URL = "io://1234-5678-3293/7614902"
LOCAL_COVER_ENTITY_ID = "cover.garden_pergola"
LOCAL_LIGHT_DEVICE_URL = "io://1234-5678-3293/14608095"


@pytest.fixture(autouse=True)
def fixture_platforms() -> Generator[None]:
    """Limit platforms to cover only."""
    with patch("homeassistant.components.overkiz.PLATFORMS", [Platform.COVER]):
        yield


async def test_local_commands_are_batched(
    hass: HomeAssistant,
    setup_overkiz_integration: SetupOverkizIntegration,
    mock_client: MockOverkizClient,
) -> None:
    """Test multiple local commands are sent as one action group."""
    config_entry = await setup_overkiz_integration(
        fixture="setup/local_somfy_tahoma_v2_europe.json",
        api_type=APIType.LOCAL,
    )
    coordinator = config_entry.runtime_data.coordinator
    mock_client.fetch_events.reset_mock()

    await asyncio.gather(
        OverkizExecutor(
            LOCAL_COVER_DEVICE_URL, coordinator
        ).async_execute_command(OverkizCommand.OPEN),
        OverkizExecutor(
            LOCAL_LIGHT_DEVICE_URL, coordinator
        ).async_execute_command(OverkizCommand.OFF),
    )

    assert mock_client.execute_action_group.await_count == 1
    kwargs = mock_client.execute_action_group.await_args.kwargs
    assert kwargs["label"] == "Home Assistant"
    actions = kwargs["actions"]
    assert len(actions) == 2
    assert actions[0].device_url == LOCAL_COVER_DEVICE_URL
    assert actions[0].commands[0].name == OverkizCommand.OPEN
    assert actions[1].device_url == LOCAL_LIGHT_DEVICE_URL
    assert actions[1].commands[0].name == OverkizCommand.OFF
    assert coordinator.executions["exec-1"] == [
        {"device_url": LOCAL_COVER_DEVICE_URL, "command_name": OverkizCommand.OPEN},
        {"device_url": LOCAL_LIGHT_DEVICE_URL, "command_name": OverkizCommand.OFF},
    ]
    assert mock_client.fetch_events.await_count == 1


async def test_cloud_commands_are_not_batched(
    setup_overkiz_integration: SetupOverkizIntegration,
    mock_client: MockOverkizClient,
) -> None:
    """Test cloud commands continue to execute immediately."""
    config_entry = await setup_overkiz_integration(
        fixture="setup/local_somfy_tahoma_v2_europe.json"
    )
    coordinator = config_entry.runtime_data.coordinator

    await asyncio.gather(
        OverkizExecutor(
            LOCAL_COVER_DEVICE_URL, coordinator
        ).async_execute_command(OverkizCommand.OPEN, refresh_afterwards=False),
        OverkizExecutor(
            LOCAL_LIGHT_DEVICE_URL, coordinator
        ).async_execute_command(OverkizCommand.OFF, refresh_afterwards=False),
    )

    assert mock_client.execute_action_group.await_count == 2


async def test_local_batch_error_propagates_to_all_callers(
    setup_overkiz_integration: SetupOverkizIntegration,
    mock_client: MockOverkizClient,
) -> None:
    """Test a queue flush error is raised to each queued caller."""
    config_entry = await setup_overkiz_integration(
        fixture="setup/local_somfy_tahoma_v2_europe.json",
        api_type=APIType.LOCAL,
    )
    coordinator = config_entry.runtime_data.coordinator
    mock_client.execute_action_group.side_effect = TooManyConcurrentRequestsError(
        "too many concurrent requests"
    )

    results = await asyncio.gather(
        OverkizExecutor(
            LOCAL_COVER_DEVICE_URL, coordinator
        ).async_execute_command(OverkizCommand.OPEN, refresh_afterwards=False),
        OverkizExecutor(
            LOCAL_LIGHT_DEVICE_URL, coordinator
        ).async_execute_command(OverkizCommand.OFF, refresh_afterwards=False),
        return_exceptions=True,
    )

    assert all(isinstance(result, HomeAssistantError) for result in results)


async def test_batch_execution_state_is_tracked_by_cover_entity(
    hass: HomeAssistant,
    setup_overkiz_integration: SetupOverkizIntegration,
    mock_client: MockOverkizClient,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Test a cover sees its command running inside a batch."""
    config_entry = await setup_overkiz_integration(
        fixture="setup/local_somfy_tahoma_v2_europe.json",
        api_type=APIType.LOCAL,
    )
    coordinator = config_entry.runtime_data.coordinator

    await hass.services.async_call(
        COVER_DOMAIN,
        SERVICE_OPEN_COVER,
        {ATTR_ENTITY_ID: LOCAL_COVER_ENTITY_ID},
        blocking=True,
    )

    assert hass.states.get(LOCAL_COVER_ENTITY_ID).state == CoverState.OPENING

    await async_deliver_events(
        hass,
        freezer,
        mock_client,
        [
            execution_state_changed_event(
                exec_id="exec-1",
                new_state=ExecutionState.COMPLETED,
                old_state=ExecutionState.IN_PROGRESS,
            )
        ],
    )

    assert "exec-1" not in coordinator.executions


async def test_single_action_tracked_execution_can_be_cancelled(
    setup_overkiz_integration: SetupOverkizIntegration,
    mock_client: MockOverkizClient,
) -> None:
    """Test single-action executions keep the existing cancellation behavior."""
    config_entry = await setup_overkiz_integration(
        fixture="setup/local_somfy_tahoma_v2_europe.json"
    )
    coordinator = config_entry.runtime_data.coordinator
    coordinator.register_execution(
        "exec-1",
        [{"device_url": LOCAL_COVER_DEVICE_URL, "command_name": OverkizCommand.OPEN}],
    )

    assert await OverkizExecutor(
        LOCAL_COVER_DEVICE_URL, coordinator
    ).async_cancel_command([OverkizCommand.OPEN])
    mock_client.cancel_execution.assert_awaited_once_with("exec-1")


async def test_multi_action_tracked_execution_is_not_cancelled(
    setup_overkiz_integration: SetupOverkizIntegration,
    mock_client: MockOverkizClient,
) -> None:
    """Test a multi-action batch is not cancelled implicitly."""
    config_entry = await setup_overkiz_integration(
        fixture="setup/local_somfy_tahoma_v2_europe.json"
    )
    coordinator = config_entry.runtime_data.coordinator
    coordinator.register_execution(
        "exec-1",
        [
            {"device_url": LOCAL_COVER_DEVICE_URL, "command_name": OverkizCommand.OPEN},
            {"device_url": LOCAL_LIGHT_DEVICE_URL, "command_name": OverkizCommand.OFF},
        ],
    )

    assert not await OverkizExecutor(
        LOCAL_COVER_DEVICE_URL, coordinator
    ).async_cancel_command([OverkizCommand.OPEN])
    mock_client.cancel_execution.assert_not_awaited()


@pytest.mark.parametrize(
    ("actions", "expected_cancelled"),
    [
        (
            [
                Action(
                    device_url=LOCAL_COVER_DEVICE_URL,
                    commands=[Command(name=OverkizCommand.OPEN, parameters=[])],
                )
            ],
            True,
        ),
        (
            [
                Action(
                    device_url=LOCAL_COVER_DEVICE_URL,
                    commands=[Command(name=OverkizCommand.OPEN, parameters=[])],
                ),
                Action(
                    device_url=LOCAL_LIGHT_DEVICE_URL,
                    commands=[Command(name=OverkizCommand.OFF, parameters=[])],
                ),
            ],
            False,
        ),
    ],
    ids=["single-action", "multi-action"],
)
async def test_external_execution_cancellation_policy(
    setup_overkiz_integration: SetupOverkizIntegration,
    mock_client: MockOverkizClient,
    actions: list[Action],
    expected_cancelled: bool,
) -> None:
    """Test external executions are only cancelled when they contain one command."""
    config_entry = await setup_overkiz_integration(
        fixture="setup/local_somfy_tahoma_v2_europe.json"
    )
    coordinator = config_entry.runtime_data.coordinator
    mock_client.get_current_executions = AsyncMock(
        return_value=[
            SimpleNamespace(
                id="exec-1",
                action_group=SimpleNamespace(actions=actions),
            )
        ]
    )

    assert (
        await OverkizExecutor(LOCAL_COVER_DEVICE_URL, coordinator).async_cancel_command(
            [OverkizCommand.OPEN]
        )
        is expected_cancelled
    )

    if expected_cancelled:
        mock_client.cancel_execution.assert_awaited_once_with("exec-1")
    else:
        mock_client.cancel_execution.assert_not_awaited()

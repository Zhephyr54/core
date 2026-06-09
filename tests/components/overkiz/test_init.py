"""Tests for Overkiz integration init."""

from unittest.mock import patch

from pyoverkiz.action_queue import ActionQueueSettings
from pyoverkiz.client import OverkizClientSettings

from homeassistant.components.overkiz import create_cloud_client, create_local_client
from homeassistant.components.overkiz.const import DOMAIN
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.setup import async_setup_component

from .test_config_flow import TEST_EMAIL, TEST_GATEWAY_ID, TEST_PASSWORD, TEST_SERVER

from tests.common import MockConfigEntry, RegistryEntryWithDefaults, mock_registry

ENTITY_SENSOR_DISCRETE_RSSI_LEVEL = "sensor.zipscreen_woonkamer_discrete_rssi_level"
ENTITY_ALARM_CONTROL_PANEL = "alarm_control_panel.alarm"
ENTITY_SWITCH_GARAGE = "switch.garage"
ENTITY_SENSOR_TARGET_CLOSURE_STATE = "sensor.zipscreen_woonkamer_target_closure_state"
ENTITY_SENSOR_TARGET_CLOSURE_STATE_2 = (
    "sensor.zipscreen_woonkamer_target_closure_state_2"
)


def _assert_action_queue_settings(settings: OverkizClientSettings) -> None:
    """Assert action queue settings are enabled."""
    assert isinstance(settings.action_queue, ActionQueueSettings)
    assert settings.action_queue.delay == 0.1
    assert settings.action_queue.max_actions == 20
    assert settings.default_rts_command_duration is None


async def test_create_local_client_enables_action_queue(hass: HomeAssistant) -> None:
    """Test local clients enable pyoverkiz action queue."""
    with patch("homeassistant.components.overkiz.OverkizClient") as client_class:
        create_local_client(hass, "gateway.local", "token", True)

    _assert_action_queue_settings(client_class.call_args.kwargs["settings"])


async def test_create_cloud_client_enables_action_queue(hass: HomeAssistant) -> None:
    """Test cloud clients enable pyoverkiz action queue."""
    with patch("homeassistant.components.overkiz.OverkizClient") as client_class:
        create_cloud_client(hass, TEST_EMAIL, TEST_PASSWORD, TEST_SERVER)

    _assert_action_queue_settings(client_class.call_args.kwargs["settings"])


async def test_unique_id_migration(hass: HomeAssistant) -> None:
    """Test migration of sensor unique IDs."""

    mock_entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=TEST_GATEWAY_ID,
        data={"username": TEST_EMAIL, "password": TEST_PASSWORD, "hub": TEST_SERVER},
        minor_version=1,
    )

    mock_entry.add_to_hass(hass)

    mock_registry(
        hass,
        {
            # This entity will be migrated to "io://1234-5678-1234/3541212-core:DiscreteRSSILevelState"
            ENTITY_SENSOR_DISCRETE_RSSI_LEVEL: RegistryEntryWithDefaults(
                entity_id=ENTITY_SENSOR_DISCRETE_RSSI_LEVEL,
                unique_id="io://1234-5678-1234/3541212-OverkizState.CORE_DISCRETE_RSSI_LEVEL",
                platform=DOMAIN,
                config_entry_id=mock_entry.entry_id,
            ),
            # This entity will be migrated to "internal://1234-5678-1234/alarm/0-TSKAlarmController"
            ENTITY_ALARM_CONTROL_PANEL: RegistryEntryWithDefaults(
                entity_id=ENTITY_ALARM_CONTROL_PANEL,
                unique_id="internal://1234-5678-1234/alarm/0-UIWidget.TSKALARM_CONTROLLER",
                platform=DOMAIN,
                config_entry_id=mock_entry.entry_id,
            ),
            # This entity will be migrated to "io://1234-5678-1234/0-OnOff"
            ENTITY_SWITCH_GARAGE: RegistryEntryWithDefaults(
                entity_id=ENTITY_SWITCH_GARAGE,
                unique_id="io://1234-5678-1234/0-UIClass.ON_OFF",
                platform=DOMAIN,
                config_entry_id=mock_entry.entry_id,
            ),
            # This entity will be removed since
            # "io://1234-5678-1234/3541212-core:TargetClosureState"
            # already exists
            ENTITY_SENSOR_TARGET_CLOSURE_STATE: RegistryEntryWithDefaults(
                entity_id=ENTITY_SENSOR_TARGET_CLOSURE_STATE,
                unique_id="io://1234-5678-1234/3541212-OverkizState.CORE_TARGET_CLOSURE",
                platform=DOMAIN,
                config_entry_id=mock_entry.entry_id,
            ),
            # This entity will not be migrated"
            ENTITY_SENSOR_TARGET_CLOSURE_STATE_2: RegistryEntryWithDefaults(
                entity_id=ENTITY_SENSOR_TARGET_CLOSURE_STATE_2,
                unique_id="io://1234-5678-1234/3541212-core:TargetClosureState",
                platform=DOMAIN,
                config_entry_id=mock_entry.entry_id,
            ),
        },
    )
    assert await async_setup_component(hass, DOMAIN, {})
    await hass.async_block_till_done()

    ent_reg = er.async_get(hass)

    unique_id_map = {
        ENTITY_SENSOR_DISCRETE_RSSI_LEVEL: "io://1234-5678-1234/3541212-core:DiscreteRSSILevelState",
        ENTITY_ALARM_CONTROL_PANEL: "internal://1234-5678-1234/alarm/0-TSKAlarmController",
        ENTITY_SWITCH_GARAGE: "io://1234-5678-1234/0-OnOff",
        ENTITY_SENSOR_TARGET_CLOSURE_STATE_2: "io://1234-5678-1234/3541212-core:TargetClosureState",
    }

    # Test if entities will be removed
    assert set(ent_reg.entities.keys()) == set(unique_id_map)

    # Test if unique ids are migrated
    for entity_id, unique_id in unique_id_map.items():
        entry = ent_reg.async_get(entity_id)
        assert entry.unique_id == unique_id

    # Test if the config entry is migrated to the latest minor version
    assert mock_entry.minor_version == 2

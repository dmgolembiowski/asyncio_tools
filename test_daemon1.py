import asyncio
import pytest
from . import daemon

@pytest.fixture
def message():
    return daemon.Message(message_id="1234", instance_name="mayhem")

@pytest.fixture
def create_mock_coro(mocker, monkeypatch):
    """Create a mock-coro pair.
    The coro can be used to patch an async method while the mock can
    be used to assert calls to the mocked out method.
    """

    def _create_mock_coro_pair(to_patch=None):
        mock = mocker.Mock()

        async def _coro(*args, **kwargs):
            return mock(*args, **kwargs)

        if to_patch:
            monkeypatch.setattr(to_patch, _coro)

        return mock, _coro

    return _create_mock_coro_pair

@pytest.mark.asyncio
async def test_save(message):
    await daemon.save(message)
    assert message.saved

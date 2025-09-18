from unittest.mock import patch
from datetime import datetime, timedelta
from neynar_parquet_importer.neynar_api import NeynarApiClient
from neynar_parquet_importer.settings import Settings


def test_get_portal_pricing():
    settings = Settings()

    client = NeynarApiClient.new(
        api_key=settings.neynar_api_key,
        api_url=settings.neynar_api_url,
    )

    x = client.get_portal_pricing("indexer_service")

    print(x)

    assert x.get("farcaster.account_verifications") is not None


def test_get_portal_pricing_cache_hit():
    settings = Settings()

    # Create a fresh client instance for this test
    client = NeynarApiClient(
        api_key=settings.neynar_api_key,
        api_url=settings.neynar_api_url,
    )

    # Mock the get method to track calls
    with patch.object(client, 'get') as mock_get:
        # Set up mock response
        mock_response = {
            "pricing": {
                "products": {
                    "data": {
                        "indexer_service": {
                            "indexer_service": {
                                "farcaster.account_verifications": {
                                    "price": {"amount": 100}
                                },
                                "farcaster.blocks": {
                                    "price": {"amount": 200}
                                }
                            }
                        }
                    }
                }
            }
        }
        mock_get.return_value = mock_response

        # First call - should hit the API
        result1 = client.get_portal_pricing("indexer_service")

        # Second call - should hit the cache
        result2 = client.get_portal_pricing("indexer_service")

        # Verify both calls return the same result
        assert result1 == result2
        assert result1["farcaster.account_verifications"] == 100

        # Verify the API was only called once (cache hit on second call)
        assert mock_get.call_count == 1, f"Expected 1 API call, but got {mock_get.call_count}"


def test_get_portal_pricing_cache_expiration():
    settings = Settings()

    # We need to control time before creating the client
    # Create a mock time function that we can control
    current_time = datetime(2024, 1, 1, 10, 0, 0)

    def mock_now():
        return current_time

    # Mock datetime.now to control time
    with patch('neynar_parquet_importer.neynar_api.datetime') as mock_datetime:
        mock_datetime.now = mock_now
        mock_datetime.timedelta = timedelta  # Keep the real timedelta

        # Create a fresh client instance with our mocked time
        client = NeynarApiClient(
            api_key=settings.neynar_api_key,
            api_url=settings.neynar_api_url,
        )

        # Mock the get method to track calls
        with patch.object(client, 'get') as mock_get:
            # Set up mock response
            mock_response = {
                "pricing": {
                    "products": {
                        "data": {
                            "indexer_service": {
                                "indexer_service": {
                                    "farcaster.account_verifications": {
                                        "price": {"amount": 100}
                                    },
                                    "farcaster.blocks": {
                                        "price": {"amount": 200}
                                    }
                                }
                            }
                        }
                    }
                }
            }
            mock_get.return_value = mock_response

            # First call - should hit the API
            result1 = client.get_portal_pricing("indexer_service")

            # Second call - within TTL (7 hours), should hit cache
            current_time = datetime(2024, 1, 1, 17, 0, 0)  # 7 hours later
            result2 = client.get_portal_pricing("indexer_service")

            # Verify cache was used (only 1 API call so far)
            assert mock_get.call_count == 1, f"Expected 1 API call after 7 hours, but got {mock_get.call_count}"

            # Third call - after TTL (more than 8 hours), should hit API again
            current_time = datetime(2024, 1, 1, 18, 1, 0)  # 8 hours and 1 minute later
            result3 = client.get_portal_pricing("indexer_service")

            # Verify all results are the same
            assert result1 == result2 == result3
            assert result1["farcaster.account_verifications"] == 100

            # Verify the API was called twice (initial call + call after cache expiration)
            assert mock_get.call_count == 2, f"Expected 2 API calls after cache expiration, but got {mock_get.call_count}"

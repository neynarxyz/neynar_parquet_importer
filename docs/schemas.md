# Database Schemas

This document organizes the available database schemas by version.

## V3 (Nindexer)

| Table | Schema File |
|-------|-------------|
| blocks | [037_00_public-postgres_nindexer_blocks.sql](../schema/037_00_public-postgres_nindexer_blocks.sql) |
| casts | [025_00_public-postgres_nindexer_casts.sql](../schema/025_00_public-postgres_nindexer_casts.sql) |
| channel_follows | [032_00_public-postgres_nindexer_channel_follows.sql](../schema/032_00_public-postgres_nindexer_channel_follows.sql) |
| channel_members | [034_00_public-postgres_nindexer_channel_members.sql](../schema/034_00_public-postgres_nindexer_channel_members.sql) |
| channels | [033_00_public-postgres_nindexer_channels.sql](../schema/033_00_public-postgres_nindexer_channels.sql) |
| fids | [029_00_public-postgres_nindexer_fids.sql](../schema/029_00_public-postgres_nindexer_fids.sql) |
| follow_counts | [015_00_public-postgres_nindexer_follow_counts.sql](../schema/015_00_public-postgres_nindexer_follow_counts.sql) |
| follows | [013_00_public-postgres_nindexer_follows.sql](../schema/013_00_public-postgres_nindexer_follows.sql) |
| neynar_user_scores | [016_00_public-postgres_nindexer_neynar_user_scores.sql](../schema/016_00_public-postgres_nindexer_neynar_user_scores.sql) |
| profile_external_accounts | [035_00_public-postgres_nindexer_profile_external_accounts.sql](../schema/035_00_public-postgres_nindexer_profile_external_accounts.sql) |
| profiles | [014_00_public-postgres_nindexer_profiles.sql](../schema/014_00_public-postgres_nindexer_profiles.sql) |
| profiles_with_verifications | [023_00_public-postgres_nindexer_profiles_with_verifications.sql](../schema/023_00_public-postgres_nindexer_profiles_with_verifications.sql) |
| reactions | [026_00_public-postgres_nindexer_reactions.sql](../schema/026_00_public-postgres_nindexer_reactions.sql) |
| signers | [030_00_public-postgres_nindexer_signers.sql](../schema/030_00_public-postgres_nindexer_signers.sql) |
| storage_rentals | [031_00_public-postgres_nindexer_storage_rentals.sql](../schema/031_00_public-postgres_nindexer_storage_rentals.sql) |
| tier_purchases | [028_00_public-postgres_nindexer_tier_purchases.sql](../schema/028_00_public-postgres_nindexer_tier_purchases.sql) |
| user_labels | [036_00_public-postgres_nindexer_user_labels.sql](../schema/036_00_public-postgres_nindexer_user_labels.sql) |
| usernames | [027_00_public-postgres_nindexer_usernames.sql](../schema/027_00_public-postgres_nindexer_usernames.sql) |
| verifications | [012_00_public-postgres_nindexer_verifications.sql](../schema/012_00_public-postgres_nindexer_verifications.sql) |

## V2 (Farcaster) - Deprecated

| Table | Schema File |
|-------|-------------|
| account_verifications | [017_00_public-postgres_farcaster_account_verifications.sql](../schema/017_00_public-postgres_farcaster_account_verifications.sql) |
| blocks | [022_00_public-postgres_farcaster_blocks.sql](../schema/022_00_public-postgres_farcaster_blocks.sql) |
| casts | [001_00_public-postgres_farcaster_casts.sql](../schema/001_00_public-postgres_farcaster_casts.sql) |
| channel_follows | [018_00_public-postgres_farcaster_channel_follows.sql](../schema/018_00_public-postgres_farcaster_channel_follows.sql) |
| channel_members | [019_00_public-postgres_farcaster_channel_members.sql](../schema/019_00_public-postgres_farcaster_channel_members.sql) |
| channels | [020_00_public-postgres_farcaster_channels.sql](../schema/020_00_public-postgres_farcaster_channels.sql) |
| fids | [002_00_public-postgres_farcaster_fids.sql](../schema/002_00_public-postgres_farcaster_fids.sql) |
| fnames | [003_00_public-postgres_farcaster_fnames.sql](../schema/003_00_public-postgres_farcaster_fnames.sql) |
| power_users | [021_00_public-postgres_farcaster_power_users.sql](../schema/021_00_public-postgres_farcaster_power_users.sql) |
| profile_with_addresses | [011_00_public-postgres_farcaster_profile_with_addresses.sql](../schema/011_00_public-postgres_farcaster_profile_with_addresses.sql) |
| reactions | [005_00_public-postgres_farcaster_reactions.sql](../schema/005_00_public-postgres_farcaster_reactions.sql) |
| signers | [006_00_public-postgres_farcaster_signers.sql](../schema/006_00_public-postgres_farcaster_signers.sql) |
| storage | [007_00_public-postgres_farcaster_storage.sql](../schema/007_00_public-postgres_farcaster_storage.sql) |
| user_data | [008_00_public-postgres_farcaster_user_data.sql](../schema/008_00_public-postgres_farcaster_user_data.sql) |
| user_labels | [024_00_public-postgres_farcaster_user_labels.sql](../schema/024_00_public-postgres_farcaster_user_labels.sql) |
| warpcast_power_users | [010_00_public-postgres_farcaster_warpcast_power_users.sql](../schema/010_00_public-postgres_farcaster_warpcast_power_users.sql) |

## Internal

| Table | Schema File |
|-------|-------------|
| parquet_import_tracking | [000_00_all_parquet_import_tracking.sql](../schema/000_00_all_parquet_import_tracking.sql) |

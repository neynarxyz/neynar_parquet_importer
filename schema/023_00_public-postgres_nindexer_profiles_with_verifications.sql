DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.views
        WHERE table_name = 'profiles_with_verifications'
        AND table_schema = '${POSTGRES_SCHEMA}'
    ) THEN
        CREATE VIEW ${POSTGRES_SCHEMA}.profiles_with_verifications AS
        SELECT 
            p.id AS profile_id,
            p.created_at AS profile_created_at,
            p.updated_at AS profile_updated_at,
            p.deleted_at AS profile_deleted_at,
            p.fid AS profile_fid,
            p.bio,
            p.pfp_url,
            p."url",
            p.username AS profile_username,
            p.display_name,
            p."location",
            p.latitude,
            p.longitude,
            -- TODO: this table is in the old schema so you need two processes
            -- (
            --     SELECT jsonb_agg(
            --         jsonb_build_object(
            --             'platform', av.platform,
            --             'platform_id', av.platform_id,
            --             'platform_username', av.platform_username,
            --             'verified_at', av.verified_at
            --         )
            --     )
            --     FROM account_verifications av
            --     WHERE av.fid = p.fid
            --     AND av.deleted_at IS NULL
            -- ) AS account_verifications,
            (
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'timestamp', v."timestamp",
                        -- TODO: solana encodes addresses with base58, not hex
                        'address', '0x' || encode(v."address", 'hex'),
                        'protocol', v.protocol
                    )
                    ORDER BY v."timestamp" DESC
                )
                FROM ${POSTGRES_SCHEMA}.verifications v
                WHERE v.fid = p.fid
                AND v.deleted_at IS NULL
            ) AS verifications
        FROM 
            ${POSTGRES_SCHEMA}.profiles p;
    END IF;
END $$;

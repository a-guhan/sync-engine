{ inputs, ... }:

{
  perSystem = { pkgs, ... }: {
    process-compose."state" = { config, pkgs, ... }:
      let
        postgres17Sync = pkgs.callPackage ./postgresql_17_sync.nix { };
      in
      {
      imports = [
        inputs.services-flake.processComposeModules.default
      ];

      services.postgres.primary = {
        enable = true;
        package = postgres17Sync;
        port = 5433;
        listen_addresses = "0.0.0.0";
        settings = {
          wal_level = "logical";
          max_replication_slots = 4;
          max_wal_senders = 4;
        };
        hbaConf = [
          {
            type = "host";
            database = "jus_sync";
            user = "jus_sync_user";
            address = "172.16.0.0/12";
            method = "scram-sha-256";
          }
        ];
        initialDatabases = [
          {
            name = "jus_sync";
            schemas = [
              ../schemas/jus_sync_dump.sql
            ];
          }
        ];
        initialScript.after = ''
          ALTER ROLE jus_sync_user WITH REPLICATION;
          \connect jus_sync
          CREATE EXTENSION IF NOT EXISTS pg_current_snapshot_full;
          ALTER TABLE IF EXISTS public.user_table REPLICA IDENTITY FULL;
          SELECT 'CREATE PUBLICATION jus_sync_pub FOR TABLE public.user_table'
          WHERE NOT EXISTS (
            SELECT 1
            FROM pg_publication
            WHERE pubname = 'jus_sync_pub'
          ) \gexec
          SELECT 'ALTER PUBLICATION jus_sync_pub ADD TABLE public.user_table'
          WHERE NOT EXISTS (
            SELECT 1
            FROM pg_publication_tables
            WHERE pubname = 'jus_sync_pub'
              AND schemaname = 'public'
              AND tablename = 'user_table'
          ) \gexec
          SELECT format(
            'SELECT * FROM pg_create_physical_replication_slot(%L, true)',
            'jus_sync_replica_slot'
          )
          WHERE NOT EXISTS (
            SELECT 1
            FROM pg_replication_slots
            WHERE slot_name = 'jus_sync_replica_slot'
          ) \gexec
        '';
      };

      settings.processes =
        let
          primary = config.services.postgres.primary;
          replicaDataDir = "./data/replica";
          replicaSocketDir = "./data/replica-socket";
          replicaSetup = pkgs.writeShellApplication {
            name = "setup-postgres-replica";
            runtimeInputs = [
              primary.package
              pkgs.coreutils
            ];
            text = ''
              set -euo pipefail
              PGDATA=$(readlink -m "${replicaDataDir}")
              export PGDATA

              if [ -f "$PGDATA/standby.signal" ]; then
                exit 0
              fi

              rm -rf "$PGDATA"
              install -d -m 700 "$PGDATA"

              pg_basebackup \
                -h 127.0.0.1 \
                -p ${toString primary.port} \
                -U jus_sync_user \
                -D "$PGDATA" \
                -R \
                -X stream \
                -S jus_sync_replica_slot

              cat >> "$PGDATA/postgresql.auto.conf" <<'EOF'
              port = '5432'
              listen_addresses = '0.0.0.0'
              hot_standby = 'on'
              hot_standby_feedback = 'on'
              wal_level = 'logical'
              max_wal_senders = '4'
              EOF
            '';
          };
          replicaStart = pkgs.writeShellApplication {
            name = "start-postgres-replica";
            runtimeInputs = [
              primary.package
              pkgs.coreutils
            ];
            text = ''
              set -euo pipefail
              PGDATA=$(readlink -m "${replicaDataDir}")
              export PGDATA
              chmod 700 "$PGDATA"
              install -d -m 700 "${replicaSocketDir}"
              postgres -k "$(readlink -m "${replicaSocketDir}")"
            '';
          };
        in
        {
          "replica-init" = {
            command = replicaSetup;
            depends_on.primary.condition = "process_healthy";
          };

          replica = {
            command = replicaStart;
            depends_on."replica-init".condition = "process_completed_successfully";
            shutdown.signal = 2;
            readiness_probe = {
              exec.command = "${primary.package}/bin/pg_isready -h 127.0.0.1 -p 5432 -d template1";
              initial_delay_seconds = 2;
              period_seconds = 10;
              timeout_seconds = 4;
              success_threshold = 1;
              failure_threshold = 5;
            };
            availability = {
              restart = "on_failure";
              max_restarts = 5;
            };
          };
        };
      };
  };
}

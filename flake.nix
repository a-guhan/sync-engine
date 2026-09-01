{
  inputs = {
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    systems.url = "github:nix-systems/default";
    process-compose-flake.url = "github:Platonic-Systems/process-compose-flake";
    services-flake.url = "github:juspay/services-flake";
  };

  outputs = inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        inputs.process-compose-flake.flakeModule
        ./nix/services.nix
      ];
      systems = import inputs.systems;
      perSystem = { pkgs, ... }: {
        packages.pg_subxip_snapshot = pkgs.callPackage ./nix/pg_subxip_snapshot.nix { };

        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            cargo
            clippy
            rust-analyzer
            rustc
            rustfmt
            just
          ];
          shellHook = ''
            export DB_HOST='127.0.0.1'
            export DB_PORT='5433'
            export DB_NAME='jus_sync'
            export DB_USER='jus_sync_user'
            export DB_PASSWORD='jus_sync_pass'
            export REPLICA_DB_HOST='127.0.0.1'
            export REPLICA_DB_PORT='5432'
            export REPLICA_DB_NAME='jus_sync'
            export REPLICA_DB_USER='jus_sync_user'
            export REPLICA_DB_PASSWORD='jus_sync_pass'
          '';
        };
      };
    };
}

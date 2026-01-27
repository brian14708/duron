{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };
  outputs =
    inputs:
    inputs.flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        inputs.treefmt-nix.flakeModule
      ];
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      perSystem =
        {
          config,
          pkgs,
          system,
          ...
        }:
        {
          devShells = {
            default = pkgs.mkShell {
              buildInputs = with pkgs; [
                pnpm
                nodejs
                (python3.withPackages (
                  p: with p; [
                    nox
                    uv
                  ]
                ))
              ];
              env = {
                UV_PYTHON = pkgs.python314.interpreter;
              };
            };
          };

          treefmt = {
            programs = {
              nixfmt.enable = true;
              ruff-check.enable = true;
              ruff-format.enable = true;
              prettier.enable = true;
            };
            settings.formatter = {
              prettier = {
                excludes = [ "tools/trace-ui/**" ];
              };
            };
          };
        };
    };
}

{
  description = "Root flake for my machines";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-unstable-small";
  };

  outputs = {nixpkgs, ...}: let
    pkgs = import nixpkgs {system = "x86_64-linux";};
  in {
    formatter.x86_64-linux = pkgs.alejandra;
    devShells.x86_64-linux.default = pkgs.mkShell {
      RUST_BACKTRACE="1";
      MIRIFLAGS="-Zmiri-env-forward=RUST_BACKTRACE -Zmiri-strict-provenance";  
      packages = with pkgs; [
        llvm
        lldb
        graphviz
      ];
    };
  };
}

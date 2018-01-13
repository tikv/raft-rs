let
  pkgs = import <nixpkgs> { overlays = [ moz ]; };
  moz = import (builtins.fetchTarball https://github.com/mozilla/nixpkgs-mozilla/archive/master.tar.gz);
in {
  devEnv = pkgs.stdenv.mkDerivation {
    name = "raft-rs";
    buildInputs = with pkgs; [
      zlib
      cmake
      gcc
      gnumake
      openssl
      go
      perl
      (rustChannelOf { channel = "nightly"; }).rust
    ];
  };
}

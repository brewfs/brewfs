{ pkgs, lib, ... }:
{
  languages.rust = {
    enable = true;
    channel = "stable";
    version = "latest";
  };

  packages = with pkgs; [
    protobuf
    pkg-config
    openssl
    git-lfs
    cmake
    python3
    nodejs_22
    jq
  ] ++ lib.optionals pkgs.stdenv.isLinux [ pkgs.fuse3 ];

  env.CARGO_INCREMENTAL = "0";
  env.CARGO_PROFILE_DEV_DEBUG = "0";
}

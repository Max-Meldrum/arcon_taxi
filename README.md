# arcon taxi


## Build


```bash
cargo build --release
```


## Running

### Before Running

Check that you have the data file and that you have cleared the 
state directory. If no state directory is specified in the ArconConf,
then the directory will be placed under the OS tempdir. On most UNIX systems,
it will be /tmp/arcon. On osx, it will be under $TMPDIR.

Lastly, Elasticsearch and Kibana has to be up and running. If you want to 
try things out without it, then simply comment out the last operator in ``src/main.rs``.

### Actually Runing

```bash
./target/release/arcon_taxi
```

## Install Shell

```bash
cargo install arcon_shell --git https://github.com/cda-group/arcon.git
```

## Build LakeSoul Dev Image

### Start build
```bash
cd {your-workspace}/LakeSoul
docker build --network=host -f docker/dev.dockerfile -t lakesoul-spark:latest
```
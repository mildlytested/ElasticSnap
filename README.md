# ElasticSnap
A utility that can be used to manipulate and copy elasticsearch snapshots directly.

This utility was written to copy a single snapshot from one repository to another repository without interacting with elasticsearch. 


## Usage:
```ElasticSnap.py --list-snapshots --folder=<foldername>```

```ElasticSnap.py --copy --src=<folder> --dst=<folder> --uuid=<snapshot_uuid>```

```ElasticSnap.py --sync --src=<folder> --dst=<folder> [--verbose]```

```ElasticSnap.py --show-missing --src=<folder> --dst=<folder>```

```ElasticSnap.py --disk-usage --folder=<folder> --uuid=<snapshot_uuid>```

```ElasticSnap.py --take-snapshot --repo=<SnapShotRepo> --name=<SnapShotName> --indices=<IndicesList>```

```ElasticSnap.py --verify-indices-snapshot --folder=<folder>```

# Air quality Vienna

The city of Vienna only provides [a half-hourly csv file](https://www.data.gv.at/katalog/dataset/d9ae1245-158e-4d79-86a4-2d9b3defbedc) of the current air quality but unfortunately no historical data. This repo provides an AWS Lambda function which queries this CSV and stores the original CSV and a JSON file which contains all valid data points to S3.

## Accessing the data

All data can be accessed here: https://s3.eu-central-1.amazonaws.com/luftguetemesswerte/

### Modifications and Reuse

If you do not like AWS and shit, just modify the script for needs. The source code runs with an [UNLICENSE](UNLICENSE.md). I would be happy if you drop me line and reuse the [UNLICENSE](UNLICENSE.md).

## Contributing

Anytime.

## Authors

* **Gerald Bäck** - *Coinomentum* - [github](https://github.com/geraldbaeck) - [blog](http://dev.baeck.at/)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This is free and unencumbered software released into the public domain - see the [UNLICENSE.md](UNLICENSE.md) file for details, because no fucks are given.

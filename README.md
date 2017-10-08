# Air quality Vienna

The city of Vienna only provides a half-hourly csv file of the current air quality but unfortunately no historical data. This repo provides an AWS Lambda function which queries this CSV and stores the original CSV and a JSON file whcih contains all valid data points to S3.

## Accessing the data

All data can be accessed here: https://s3.eu-central-1.amazonaws.com/luftguetemesswerte/

### Modifications and Reuse

If you do not like AWS and shit, just modify the script for needs. The source code runs with an [UNLICENSE.md](UNLICENSE.md). I would be happy if you drop me line and reuse the [UNLICENSE.md](UNLICENSE.md).

## Contributing

Anytime.

## Authors

* **Gerald Bäck** - *Coinomentum* - [github](https://github.com/geraldbaeck) - [blog](http://dev.baeck.at/)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This is free and unencumbered software released into the public domain - see the [UNLICENSE.md](UNLICENSE.md) file for details.

// dependencies
var async     = require('async')
    , AWS     = require('aws-sdk')
    , gm      = require('gm').subClass({imageMagick: true})
    , fs      = require('fs')
    , util    = require('util')
    , request = require('request')
    , config  = require('config')
    , child_process = require("child_process");

// constants
var MAX_WIDTH  = 100
  , MAX_HEIGHT = 100;

AWS.config.loadFromPath('./config/aws.config.json');
// get reference to S3 client 
var s3 = new AWS.S3();
 
exports.handler = function(event, context) {
  // Read options from the event.
  console.log("Reading options from event:\n", util.inspect(event, {depth: 5}));
  var srcBucket = event.Records[0].s3.bucket.name;
  var srcKey    = event.Records[0].s3.object.key;
  // var dstBucket = srcBucket + "-thumbnails";
  var dstBucket = "thumbnails.store";
  var dstKey = srcKey + "-thumbnails";

  // Sanity check: validate that source and destination are different buckets.
  if (srcBucket === dstBucket) {
    console.error("Destination bucket must not match source bucket.");
    return;
  }

  // Infer the image type.
  var typeMatch = srcKey.match(/\.([^.]*)$/);
  if (!typeMatch) {
    console.warn('unable to infer image type for key ' + srcKey);
    // return;
  }

  var validImageTypes = [
      'png', 'jpg', 'jpeg'
  ];
  var imageType = typeMatch ? typeMatch[1] : null;
  if (imageType && validImageTypes.indexOf(imageType.toLowerCase()) < 0) {
    console.log('skipping non-image ' + srcKey);
    return;
  }

  // Download the image from S3, transform, and upload to a different S3 bucket.
  async.waterfall([
    function download(next) {
      // Download the image from S3 into a buffer.
      // s3.getObject({
      //   Bucket : srcBucket,
      //   Key : srcKey
      // }, function (err, data) {
      //   next(null, data);
      // });
      s3.getSignedUrl("getObject",{
        Bucket: srcBucket, Key: srcKey, Expires: 900
      }, function (err, data) {
        next(null, data);
      });
    },
    function transform(response, next) {
      var tmpFile = fs.createWriteStream("/tmp/" + dstKey);

      var headObject = s3.headObject({
        Bucket : srcBucket,
        Key : srcKey
      }, function (err, data) {
        if (data['ContentType'].split('/')[0] === 'image') {
          transformImage(response, next);
        } else if (data['ContentType'].split('/')[0] === 'video') {
          transformVideo(response, next);
        }
      });

      function transformImage(response, next) {
        gm(request(response)).size({bufferStream: true}, function(err, size) {
          // Infer the scaling factor to avoid stretching the image unnaturally.
          var scalingFactor = Math.min(
            MAX_WIDTH / size.width,
            MAX_HEIGHT / size.height
          );
          var width  = scalingFactor * size.width;
          var height = scalingFactor * size.height;

          if (!imageType) {
            imageType = "jpg";
          }
          // Transform the image buffer in memory.
          this.resize(width, height);
          this.write("/tmp/" + dstKey, function (err) {
            if (!err) {
              next(null);
            }
            next(null, err);
          });
        });
      }

      function transformVideo(response, next) {
        var ffmpeg = child_process.spawn("ffmpeg", [
          "-ss", "00:00:02", // time to take screenshot
          "-i", response,
          "-vf", "thumbnail,scale=" + MAX_WIDTH + ":" + MAX_HEIGHT,
          "-qscale:v", "2",
          "-frames:v", "1",
          "-f", "image2",
          "-c:v", "mjpeg",
          "-y", "pipe:1"
        ]);

        ffmpeg.stdout.on('data', function (data) {
          console.log('stdout: ' + data);
        });

        ffmpeg.stdout.pipe(tmpFile)
          .on("error", function (err) {
            console.log("error while writing: ", err);
          });

        ffmpeg.on("error", function (err) {
          console.log(err);
        });
        ffmpeg.on("close", function (code) {
          if (code !== 0) {
            console.log("child process exited with code " + code);
          } else {
            console.log("Processing finished !");
          }
          tmpFile.end();
          next(null, code);
        });
      }
    },
    function upload(data, next) {
      var tmpFile = fs.createReadStream("/tmp/" + dstKey);
      // Stream the transformed image to a different S3 bucket.
      s3.putObject({
        Bucket : dstBucket,
        Key : dstKey,
        Body : tmpFile,
        ContentType : "image/jpg",
        ACL: "public-read",
        Metadata: {
          thumbnail: "TRUE"
        }
      }, next);
    }],
    function (err) {
      if (err) {
        console.error(
          'Unable to resize ' + srcBucket + '/' + srcKey +
          ' and upload to ' + dstBucket + '/' + dstKey +
          ' due to an error: ' + err
        );
        context.done();
      } else {
        console.log(
          'Successfully resized ' + srcBucket + '/' + srcKey +
          ' and uploaded to ' + dstBucket + '/' + dstKey
        );

        // hash-fileId.ext
        // var fileMatch = srcKey.match(/\-([^.]*)\./);
        //
        // if (!fileMatch) {
        //   context.done();
        // } else {
        //   var fileId = fileMatch[1];
        //
        //   var bucketConfig = config.buckets[srcBucket];
        //   request.post(bucketConfig.host + '/api/files/' + fileId + '/thumbnail', {
        //     form : {
        //       bucket : bucketConfig.bucket,
        //       secret : bucketConfig.secret
        //     }
        //   }, function(err, response, body) {
        //     err && console.log('could not make request back: ' + err);
        //     context.done();
        //   });
        // }
      }
    }
  );
};

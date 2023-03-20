using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace HS.IO.S3
{
    public class S3IOAdapter : IOAdapter
    {
        public override char SeparatorChar => '/';
        public override bool CanRead => true;
        public override bool CanWrite => true;
        public override bool CanAppend => true;

        public AmazonS3Client S3Client { get; private set; }
        public string Bucket { get; private set; }

        public S3IOAdapter(AmazonS3Client S3Client, string Bucket)
        {
            this.S3Client = S3Client;
            this.Bucket = Bucket;
        }

        public override List<string> GetItems(string Path, ItemType Type, string Extension = null)
        {
            throw new NotImplementedException();
        }

        public override IOItemInfo GetInfo(string Path) => GetInfoAsync(Path).GetAwaiter().GetResult();
        public override async Task<IOItemInfo> GetInfoAsync(string Path) => new S3IOItemInfo(Path, await S3Client.GetObjectMetadataAsync(Bucket, Path));

        public override void Delete(string Path, IOItemKind Kind = IOItemKind.None)
        {
            throw new NotImplementedException();
        }

        public override bool Exist(string Path)
        {
            throw new NotImplementedException();
        }

        public override void CreateDirectory(string Path)
        {
            throw new NotImplementedException();
        }

        public override Stream Create(string Path)
        {
            throw new NotImplementedException();
        }

        public override Stream Append(string Path)
        {
            throw new NotImplementedException();
        }

        public override void Write(string Path, Stream Data) => WriteAsync(Path, Data).GetAwaiter().GetResult();
        public override Task WriteAsync(string Path, Stream Data)
        {
            var request = new PutObjectRequest
            {
                BucketName = Bucket,
                Key = Path,
                InputStream = Data,
                AutoCloseStream = false,
                AutoResetStreamPosition = false
            };

            return S3Client.PutObjectAsync(request);
        }

        public override void Move(string OriginalPath, string DestinationPath)
        {
            throw new NotImplementedException();
        }

        public override void SetTimestamp(string Path, DateTime Timestamp, IOItemKind Kind = IOItemKind.None)
        {
            throw new NotImplementedException();
        }

        public override Stream Open(string Path)
        {
            throw new NotImplementedException();
        }
    }
}

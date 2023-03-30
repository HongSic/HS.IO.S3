using Amazon.S3;
using Amazon.S3.Model;
using HS.Utils.Text;
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
        public override bool CanChangeTimsstamp => false;

        public AmazonS3Client S3Client { get; private set; }
        public string Bucket { get; private set; }

        public S3IOAdapter(AmazonS3Client S3Client, string Bucket)
        {
            this.S3Client = S3Client;
            this.Bucket = Bucket;
        }

        public override List<string> GetItems(string Path, ItemType Type, string Extension = null) => GetItemsAsync(Path, Type, Extension).GetAwaiter().GetResult();
        public override async Task<List<string>> GetItemsAsync(string Path, ItemType Type, string Extension = null)
        {
            string dir = StringUtils.PathMaker(Path, "/");

            var response = await S3Client.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = Bucket,
                Prefix = dir,
                MaxKeys = 1000,
                //Delimiter = "/",
            });

            var list = response.S3Objects;
            List<string> items = new List<string>(list.Count - 1);
            for (int i = 1; i < list.Count; i++)
            {
                if(Type != ItemType.All)
                {
                    bool isDirectory = list[i].Key[list[i].Key.Length - 1] == '/';
                    if ((Type == ItemType.File && isDirectory) ||
                        (Type == ItemType.Directory && !isDirectory)) continue;
                }
                items.Add(list[i].Key);
            }
            return items;
        }

        public override IOItemInfo GetInfo(string Path) => GetInfoAsync(Path).GetAwaiter().GetResult();
        public override async Task<IOItemInfo> GetInfoAsync(string Path) => new S3IOItemInfo(Path, await S3Client.GetObjectMetadataAsync(Bucket, Path));

        public override void Delete(string Path) => DeleteAsync(Path).GetAwaiter().GetResult();
        public override async Task DeleteAsync(string Path) => await S3Client.DeleteObjectAsync(Bucket, Path);

        public override bool Exist(string Path) => ExistAsync(Path).GetAwaiter().GetResult();
        public override async Task<bool> ExistAsync(string Path) => await GetInfoAsync(Path) != null || await GetInfoAsync(StringUtils.PathMaker(Path, "/")) != null;

        public override void CreateDirectory(string Path) => CreateDirectoryAsync(Path).GetAwaiter().GetResult();
        public override Task CreateDirectoryAsync(string Path) => _Create(StringUtils.PathMaker(Path, "/"));

        public override void Create(string Path) => CreateAsync(Path).GetAwaiter().GetResult();
        public override async Task CreateAsync(string Path) => await _Create(Path);
        private async Task _Create(string Path)
        {
            if (Path[Path.Length - 1] == SeparatorChar) throw new IOException("Path does not allow directory!!", unchecked((int)0x80070057));
            await S3Client.PutObjectAsync(new PutObjectRequest()
            {
                BucketName = Bucket,
                Key = Path,
                StorageClass = S3StorageClass.Standard,
                ServerSideEncryptionMethod = ServerSideEncryptionMethod.None,
                ContentBody = string.Empty
            });
        }

        public override void Write(string Path, Stream Data) => WriteAsync(Path, Data).GetAwaiter().GetResult();
        public override Task WriteAsync(string Path, Stream Data)
        {
            return S3Client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = Bucket,
                Key = Path,
                InputStream = Data,
                AutoCloseStream = false,
                AutoResetStreamPosition = false,
                ServerSideEncryptionMethod = ServerSideEncryptionMethod.None,
            });
        }

        public override void Copy(string OriginalPath, string DestinationPath) => CopyAsync(OriginalPath, DestinationPath).GetAwaiter().GetResult();
        public override Task CopyAsync(string OriginalPath, string DestinationPath) => S3Client.CopyObjectAsync(Bucket, OriginalPath, Bucket, DestinationPath);
        public Task CopyAsync(string OriginalPath, string DestinationPath, string DestinationBucket) => S3Client.CopyObjectAsync(Bucket, OriginalPath, DestinationBucket, DestinationPath);

        public override Stream Open(string Path) => OpenAsync(Path).GetAwaiter().GetResult();
        public override async Task<Stream> OpenAsync(string Path)
        {
            var request = new GetObjectRequest
            {
                BucketName = Bucket,
                Key = Path
            };
            return new S3IOStream(await S3Client.GetObjectAsync(request));
        }

        public override void Append(string Path, Stream Data) => AppendAsync(Path, Data).GetAwaiter().GetResult();
        public override Task AppendAsync(string Path, Stream Data) => AppendAsync(Path, Data, null);
        public Task AppendAsync(string Path, Stream Data, string VersionID)
        {
            //var info = (S3IOItemInfo)await GetInfoAsync(Path);
            throw new NotImplementedException();
        }
        public override void SetTimestamp(string Path, DateTime Timestamp, IOItemKind Kind = IOItemKind.None)
        {
            //User-defined object metadata -> https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#UserMetadata
            throw new NotSupportedException("AWS S3 can't modify timestamp!!");
        }
        public override void Dispose()
        {
            S3Client?.Dispose();
        }
        ~S3IOAdapter() => Dispose();
    }
}

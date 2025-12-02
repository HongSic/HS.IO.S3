using Amazon.S3;
using Amazon.S3.Model;
using HS.Utils.Text;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
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
        public override async Task<List<string>> GetItemsAsync(string Path, ItemType Type, CancellationToken cancellationToken, string Extension = null)
        {
            string dir = StringUtils.PathMaker(Path, "/");

            var response = await S3Client.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = Bucket,
                Prefix = dir,
                MaxKeys = 1000,
                //Delimiter = "/",
            }, cancellationToken);

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
        public override async Task<IOItemInfo> GetInfoAsync(string Path, CancellationToken cancellationToken)
        {
            try { return new S3IOItemInfo(Path, await S3Client.GetObjectMetadataAsync(Bucket, Path, cancellationToken)); }
            catch (AmazonS3Exception ex)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.NotFound) return null;
                else throw;
            }
        }

        public override void Delete(string Path) => DeleteAsync(Path).GetAwaiter().GetResult();
        public override Task DeleteAsync(string Path, CancellationToken cancellationToken) => S3Client.DeleteObjectAsync(Bucket, Path, cancellationToken);
        public override void DeleteDirectory(string Path) => DeleteDirectoryAsync(Path).GetAwaiter().GetResult();
        public override Task DeleteDirectoryAsync(string Path, CancellationToken cancellationToken) => S3Client.DeleteObjectAsync(Bucket, StringUtils.PathMaker(Path, "/"), cancellationToken);

        public override bool Exist(string Path) => ExistAsync(Path).GetAwaiter().GetResult();
        public override async Task<bool> ExistAsync(string Path, CancellationToken cancellationToken) => await GetInfoAsync(Path, cancellationToken) != null;
        public override bool ExistDirectory(string Path) => ExistDirectoryAsync(Path).GetAwaiter().GetResult();
        public override async Task<bool> ExistDirectoryAsync(string Path, CancellationToken cancellationToken) => await GetInfoAsync(StringUtils.PathMaker(Path, "/"), cancellationToken) != null;

        public override void CreateDirectory(string Path) => CreateDirectoryAsync(Path).GetAwaiter().GetResult();
        public override Task CreateDirectoryAsync(string Path, CancellationToken cancellationToken) => _Create(StringUtils.PathMaker(Path, "/"), false, cancellationToken);


        public override void Create(string Path) => CreateAsync(Path).GetAwaiter().GetResult();
        public override Task CreateAsync(string Path, CancellationToken cancellationToken) => _Create(Path, true, cancellationToken);
        private async Task _Create(string Path, bool IsFile, CancellationToken cancellationToken)
        {
            if (IsFile && Path[Path.Length - 1] == SeparatorChar) throw new IOException("Path does not allow directory!!", unchecked((int)0x80070057));
            await S3Client.PutObjectAsync(new PutObjectRequest()
            {
                BucketName = Bucket,
                Key = Path,
                StorageClass = S3StorageClass.Standard,
                ServerSideEncryptionMethod = ServerSideEncryptionMethod.None,
                ContentBody = string.Empty
            }, cancellationToken);
        }

        public override void Write(string Path, Stream Data) => WriteAsync(Path, Data).GetAwaiter().GetResult();
        public override Task WriteAsync(string Path, Stream Data, CancellationToken cancellationToken)
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
        public override Task CopyAsync(string OriginalPath, string DestinationPath, CancellationToken cancellationToken) => S3Client.CopyObjectAsync(Bucket, OriginalPath, Bucket, DestinationPath, cancellationToken);
        public Task CopyAsync(string OriginalPath, string DestinationPath, string DestinationBucket, CancellationToken cancellationToken) => S3Client.CopyObjectAsync(Bucket, OriginalPath, DestinationBucket, DestinationPath, cancellationToken);

        public override Stream Open(string Path) => OpenAsync(Path).GetAwaiter().GetResult();
        public override async Task<Stream> OpenAsync(string Path, CancellationToken cancellationToken)
        {
            var request = new GetObjectRequest
            {
                BucketName = Bucket,
                Key = Path
            };
            return new S3IOStream(await S3Client.GetObjectAsync(request, cancellationToken));
        }

        public override void Append(string Path, Stream Data) => AppendAsync(Path, Data).GetAwaiter().GetResult();
        public override Task AppendAsync(string Path, Stream Data, CancellationToken cancellationToken) => AppendAsync(Path, Data, null, cancellationToken);
        public Task AppendAsync(string Path, Stream Data, string VersionID, CancellationToken cancellationToken)
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

        ~S3IOAdapter() { Dispose(); }
    }
}

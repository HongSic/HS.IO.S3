using Amazon.S3.Model;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace HS.IO.S3
{
    public class S3IOStream : Stream
    {
        private readonly GetObjectResponse Get;
        public S3IOStream(GetObjectResponse Get)
        {
            this.Get = Get;
            BaseStream = Get.ResponseStream;
        }

        public Stream BaseStream { get; private set; }


        public override bool CanRead => BaseStream.CanRead;

        public override bool CanSeek => BaseStream.CanSeek;

        public override bool CanWrite => BaseStream.CanWrite;

        public override long Length => BaseStream.Length;

        public override long Position { get => BaseStream.Position; set => BaseStream.Position = value; }

        public override void Flush() => BaseStream.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => BaseStream.FlushAsync();

        public override int Read(byte[] buffer, int offset, int count) => BaseStream.Read(buffer, offset, count);
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => BaseStream.ReadAsync(buffer, offset, count, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) => BaseStream.Seek(offset, origin);

        public override void SetLength(long value) => BaseStream.SetLength(value);

        public override void Write(byte[] buffer, int offset, int count) => BaseStream.Write(buffer, offset, count);
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => BaseStream.WriteAsync(buffer, offset, count, cancellationToken);

        public override void Close()
        {
            BaseStream.Close();
            Get.Dispose();
            base.Close();
        }

        #region Override
        public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken) => BaseStream.CopyToAsync(destination, bufferSize, cancellationToken);

        public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state) => BaseStream.BeginRead(buffer, offset, count, callback, state);
        public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state) => BaseStream.BeginWrite(buffer, offset, count, callback, state);

        public override int EndRead(IAsyncResult asyncResult) => BaseStream.EndRead(asyncResult);
        public override void EndWrite(IAsyncResult asyncResult) => BaseStream.EndWrite(asyncResult);

        public override int ReadTimeout { get => BaseStream.ReadTimeout; set => BaseStream.ReadTimeout = value; }
        public override int WriteTimeout { get => BaseStream.WriteTimeout; set => BaseStream.WriteTimeout = value; }

        public override bool CanTimeout => BaseStream.CanTimeout;
        #endregion
    }
}
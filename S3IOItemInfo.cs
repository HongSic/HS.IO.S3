using Amazon.S3.Model;
using HS.Utils;
using System;

namespace HS.IO.S3
{
    public class S3IOItemInfo : IOItemInfo
    {
        internal S3IOItemInfo(string Path, GetObjectMetadataResponse Metadata) : base(Path)
        {
            this.Metadata = Metadata;
            _Length = Metadata.ContentLength;
            _ModifyTime = Metadata.LastModified;

            var split = Metadata.Headers.ContentType.Split(';');
            _Type = split[0].Trim();
            _Kind = _Type.ToLower() == "application/x-directory" ? IOItemKind.Directory : IOItemKind.File;
            VersionID = Metadata.VersionId;
        }

        private string _Type;
        private IOItemKind _Kind;
        private long _Length;
        private DateTime _ModifyTime;
        private DateTime _CreateTime;

        public GetObjectMetadataResponse Metadata { get; private set; }

        public override string Type => _Type;
        public override IOItemKind Kind => _Kind;
        public override long Length => _Length;
        public override DateTime ModifyTime => _ModifyTime;
        public override DateTime CreateTime => _CreateTime;

        public string VersionID { get; private set; }

        public override string ToString()
        {
            //string name = string.IsNullOrEmpty(Extension) ? Name : string.Format("{0}.{1}", Name, Extension);
            if (Kind == IOItemKind.File) return string.Format("[F] {0} ({1})", Path, ByteMeasure.NetworkString(Length, false, true, false, 2));
            else return $"[D] {Path}";
        }
    }
}

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.File;
using System;
using System.IO;
using System.Threading.Tasks;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Net;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Security.Authentication;
using System.Security.Cryptography;
using Microsoft.WindowsAzure.Storage.Auth;
//-p "\\CICCIANTE\Immagini" -fake

namespace SyncFiles
{
    class Program
    {
        static string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=memoriesanonima;AccountKey=J/mhCAFAN0jlFyZ4ZBKzKOhzEECHEatt9etV2SgUqBVfxe7E0jY0VZFWDZc/hzEvEF6X35QQHyOkVUsgQlvdJQ==;EndpointSuffix=core.windows.net";
 
        static string shareReference = "memories";
        //static string shareReference = "Documents";
        
        private static readonly OperationContext oc = new OperationContext();

        static void Main(string[] args)
        {
            var pars = args.ToKeyValues();
            Console.WriteLine(pars["-p"]);

            _startDir = pars["-p"];

            MainSendToCloud(_startDir, _startDir+pars["-s"]);
        }

        private static void AddProperties()
        {
            //var credentials = new StorageCredentials("bacpacimportprod", "4LHk6SOlkoRTgOsZeNpZfZLlzRrC7yZn3hwjZn9VYlIsvQpwKEQ6n4mXcAhuHcXXewuAIOlHNbahq2OEVO5p6A==");
            var credentials = new StorageCredentials("showmeplatform", "zuyWF7uMWCGNXZDRFyKvGV/+fejVPydNeDwqekIlt9o6nmy0Iqzm2DReoKw+VmVTFslfVg/p1AX5ukpwc6IF0Q==");
            var account = new CloudStorageAccount(credentials, true);

            //string assetTest = "SharedAccessSignature=sv=2019-02-02&ss=btqf&srt=sco&st=2020-07-07T11%3A59%3A56Z&se=2020-07-08T11%3A59%3A56Z&sp=rl&sig=Fi982zUjGxsOQojvrs3Q%2Bh3hX9nEyQCyiZ14dnAy0Uk%3D;BlobEndpoint=https://bacpacimportprod.blob.core.windows.net/;FileEndpoint=https://bacpacimportprod.file.core.windows.net/;QueueEndpoint=https://bacpacimportprod.queue.core.windows.net/;TableEndpoint=https://bacpacimportprod.table.core.windows.net/";
            //CloudStorageAccount storageAccount = CloudStorageAccount.Parse(assetTest);

            var client = account.CreateCloudBlobClient();
            var properties = client.GetServicePropertiesAsync().Result;
            properties.DefaultServiceVersion = "2019-07-07";
            client.SetServicePropertiesAsync(properties).Wait();
        }

        static void MainSendToCloud(string path, string filter)
        {
            CloudFileDirectory root = GetRoot("Pictures");

            OperationContext.GlobalRequestCompleted += new EventHandler<RequestEventArgs>((req, e) =>
            {
                var r = req as Microsoft.WindowsAzure.Storage.OperationContext;
                Console.WriteLine(" " + r.LastResult.EndTime.ToString() + "  " + r.LastResult.ContentMd5 + "  " + e.RequestUri.LocalPath);
            });
            var f = Directories(path, filter, root).Result;
        }

        public static CloudFileDirectory GetRoot(string sub)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            CloudFileClient fileClient = storageAccount.CreateCloudFileClient();
            CloudFileShare share = fileClient.GetShareReference(shareReference.ToLower());
            CloudFileDirectory root = share.GetRootDirectoryReference();
            // CloudFileDirectory cloudPictures = root.GetDirectoryReference("Pictures");
            CloudFileDirectory cloudPictures = root.GetDirectoryReference(sub);
            return cloudPictures;
        }

        private static string _startDir;

        public static async Task<string> Directories(string startDir, string filter, CloudFileDirectory dirBase)
        {
            foreach (var dir in Directory.GetDirectories(startDir))
            {
                try
                {
                    var ff = Path.GetFileName(dir);
                    CloudFileDirectory dirCloud = dirBase.GetDirectoryReference(ff);
                    if (!dir.StartsWith(filter))
                    {
                        await Directories(dir, filter, dirCloud);
                        continue;
                    }
                    await dirCloud.CreateIfNotExistsAsync();
                    var list = await dirCloud.ListFilesAndDirectoriesSegmentedAsync(int.MaxValue, new FileContinuationToken(), new FileRequestOptions() { StoreFileContentMD5 = true }, oc);
                    //var list = await dirCloud.ListFilesAndDirectoriesSegmentedAsync(new FileContinuationToken());
                    foreach (var fileName in Directory.GetFiles(dir))
                    {
                        //Console.WriteLine(DateTime.Now.ToString() + " CHECK " + fileName);
                        FileInfo file = new FileInfo(fileName);
                        var blobFile = list.Results.OfType<CloudFile>().FirstOrDefault(s => s.Name == file.Name);
                        if (blobFile != null)
                        {
                            await blobFile.FetchAttributesAsync();
                        }
                        if (blobFile == null || blobFile.Properties.ContentMD5 == null || file.Length != blobFile.Properties.Length)
                        {
                            Console.WriteLine(DateTime.Now.ToString() + " START " + file.Name);
                            var f = await UploadFile(file, dirCloud);
                        }
                    }
                    await Directories(dir, filter, dirCloud);
                }
                catch (Exception xx)
                {
                    Console.WriteLine($"FAIL {xx.Message}");
                }
            }
            return "ok";
        }

        public static async Task<string> UploadFile(FileInfo file, CloudFileDirectory dirCloud)
        {
            try
            {
                CloudFile cloudFile = dirCloud.GetFileReference(file.Name);
                using (FileStream fs = file.OpenRead())
                {
                    if(file.Extension.ToUpper() == ".CR2")
                        cloudFile.Properties.ContentType = "image/x-canon-cr2";
                    if(file.Extension.ToUpper() == ".MP4")
                        cloudFile.Properties.ContentType = "video/mp4";
                    if(file.Extension.ToUpper() == ".JPEG")
                        cloudFile.Properties.ContentType = "image/jpeg";
                    if(file.Extension.ToUpper() == ".JPG")
                        cloudFile.Properties.ContentType = "image/jpeg";
                    await cloudFile.UploadFromStreamAsync(fs, AccessCondition.GenerateEmptyCondition(), new FileRequestOptions() { StoreFileContentMD5 = true }, oc);
                }
                return file.FullName;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"retry {file.FullName}, {ex.Message}");
                if (ex.Message != "The specified resource is read-only and cannot be modified at this time.")
                {
                    return await UploadFile(file, dirCloud);
                }
                return "KO";
            }
        }

        static string CalculateMD5(string filename)
        {

                using (var stream = File.OpenRead(filename))
                {
                    byte[] retrievedBuffer = new byte[stream.Length];
                    stream.Read(retrievedBuffer, 0, (int)stream.Length);
                                // Validate MD5 Value
                    var md5Check = System.Security.Cryptography.MD5.Create();
                    md5Check.TransformBlock(retrievedBuffer, 0, retrievedBuffer.Length, null, 0);     
                    md5Check.TransformFinalBlock(new byte[0], 0, 0);

                    // Get Hash Value
                    byte[] hashBytes = md5Check.Hash;
                    string hashVal = Convert.ToBase64String(hashBytes);

                    return hashVal;
            }
            // using (var md5 = MD5.Create())
            // {
            //     using (var stream = File.OpenRead(filename))
            //     {
            //         var hash = md5.ComputeHash(stream);
            //         return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
            //     }
            // }
        }

        public static void RenameFolders()
        {
            List<string> dirs = Directory.EnumerateDirectories("I:\\4TB\\Immagini\\2018").ToList();
            foreach (var dir in dirs)
            {
                try
                {
                    var dateDir = DateTime.ParseExact(dir.Replace(Path.GetDirectoryName(dir) + "\\", ""), "dd-MM-yyyy", null);
                    Directory.Move(dir, Path.GetDirectoryName(dir) + "\\" + dateDir.ToString("yyyy-MM-dd"));
                }
                catch (Exception)
                {
                }
            }
        }
    }

    public static class MyLoDash
    {
        public static Dictionary<string, string> ToKeyValues(this string[] array)
        {
            var length = array.Length - array.Length % 2;
            var res = new Dictionary<string, string>();
            for (int i = 0; i <= length / 2; i = i + 2)
            {
                res.Add(array[i], array[i + 1]);
            }
            if (array.Length % 2 == 1)
            {
                res.Add(array[array.Length - 1], "");
            }
            return res;
        }
    }
}

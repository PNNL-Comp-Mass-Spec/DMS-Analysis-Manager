using System;
using AnalysisManagerBase;
using NUnit.Framework;

namespace AnalysisManagerTest
{
    [TestFixture]
    public class PathUtilTests
    {
        [Test]
        [TestCase(@"C:\Users\Public\Pictures", @"C:\Users\Public", "Pictures")]
        [TestCase(@"C:\Users\Public\Pictures\", @"C:\Users\Public", "Pictures")]
        [TestCase(@"C:\Windows\System32", @"C:\Windows", "System32")]
        [TestCase(@"C:\Windows", @"C:\", "Windows")]
        [TestCase(@"C:\Windows\", @"C:\", "Windows")]
        [TestCase(@"C:\", @"", @"C:\")]
        [TestCase(@"Microsoft SQL Server\Client SDK\ODBC", @"Microsoft SQL Server\Client SDK", "ODBC")]
        [TestCase(@"TortoiseGit\bin", @"TortoiseGit", "bin")]
        [TestCase(@"TortoiseGit\bin\", @"TortoiseGit", "bin")]
        [TestCase(@"TortoiseGit", @"", "TortoiseGit")]
        [TestCase(@"TortoiseGit\", @"", "TortoiseGit")]
        [TestCase(@"\\server\Share\Folder", @"\\server\Share", "Folder")]
        [TestCase(@"\\server\Share\Folder\", @"\\server\Share", "Folder")]
        [TestCase(@"\\server\Share", @"", "Share")]
        [TestCase(@"\\server\Share\", @"", "Share")]
        [TestCase(@"/etc/fonts/conf.d", @"/etc/fonts", "conf.d")]
        [TestCase(@"/etc/fonts/conf.d/", @"/etc/fonts", "conf.d")]
        [TestCase(@"/etc/fonts", @"/etc", "fonts")]
        [TestCase(@"/etc", @"/", "etc")]
        [TestCase(@"/etc/", @"/", "etc")]
        [TestCase(@"/", @"", "")]
        [TestCase(@"log/xymon", @"log", "xymon")]
        [TestCase(@"log/xymon/old", @"log/xymon", "old")]
        [TestCase(@"log", @"", "log")]
        public void TestGetParentDirectoryPath(string directoryPath, string expectedParentPath, string expectedDirectoryName)
        {
            var parentPath = clsPathUtils.GetParentDirectoryPath(directoryPath, out var directoryName);


            if (string.IsNullOrWhiteSpace(parentPath))
            {
                Console.WriteLine("{0} has no parent; name is {1}", directoryPath, directoryName);
            }
            else
            {
                Console.WriteLine("{0} has parent {1} and name {2}", directoryPath, parentPath, directoryName);
            }

            Assert.AreEqual(parentPath, expectedParentPath, "Parent path mismatch");
            Assert.AreEqual(directoryName, expectedDirectoryName, "Directory name mismatch");
        }
    }
}

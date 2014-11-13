'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/29/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Runtime.InteropServices

Public Class clsCreateMSGFDBSuffixArrayFiles

#Region "Constants"
	Public Const LEGACY_MSGFDB_SUBDIRECTORY_NAME As String = "Legacy_MSGFDB"
	Protected Const MSGF_PLUS_INDEX_FILE_INFO_SUFFIX As String = ".MSGFPlusIndexFileInfo"
#End Region

#Region "Module Variables"
	Protected mErrorMessage As String = String.Empty
	Protected mMgrName As String

	Protected mPICHPCUser As String
	Protected mPICHPCPassword As String

	Protected WithEvents mComputeCluster As HPC_Submit.WindowsHPC2012
#End Region

	Public ReadOnly Property ErrorMessage As String
		Get
			Return mErrorMessage
		End Get
	End Property

	Public Sub New(ByVal strManagerName As String)
		Me.New(strManagerName, String.Empty, String.Empty)
	End Sub

	Public Sub New(ByVal strManagerName As String, ByVal sPICHPCUser As String, ByVal sPICHPCPassword As String)
		mMgrName = strManagerName
		mPICHPCUser = sPICHPCUser
		mPICHPCPassword = sPICHPCPassword
	End Sub

	Protected Function CopyExistingIndexFilesFromRemote(
	  ByVal fiFastaFile As FileInfo,
	  ByVal blnUsingLegacyFasta As Boolean,
	  ByVal strRemoteIndexFolderPath As String,
	  ByVal blnCheckForLockFile As Boolean,
	  ByVal intDebugLevel As Integer,
	  ByVal sngMaxWaitTimeHours As Single,
	  <Out()> ByRef diskFreeSpaceBelowThreshold As Boolean) As IJobParams.CloseOutType

		Dim blnSuccess As Boolean = False

		diskFreeSpaceBelowThreshold = False

		Try

			Dim diRemoteIndexFolderPath As DirectoryInfo
			diRemoteIndexFolderPath = New DirectoryInfo(strRemoteIndexFolderPath)

			If Not diRemoteIndexFolderPath.Exists Then
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If

			If blnCheckForLockFile Then
				' Look for an existing lock file
				Dim fiRemoteLockFile1 As FileInfo
				fiRemoteLockFile1 = New FileInfo(Path.Combine(diRemoteIndexFolderPath.FullName, fiFastaFile.Name & MSGF_PLUS_INDEX_FILE_INFO_SUFFIX & ".lock"))

				WaitForExistingLockfile(fiRemoteLockFile1, intDebugLevel, sngMaxWaitTimeHours)

			End If

			' Look for the .MSGFPlusIndexFileInfo file for this fasta file
			Dim fiMSGFPlusIndexFileInfo As FileInfo
			fiMSGFPlusIndexFileInfo = New FileInfo(Path.Combine(diRemoteIndexFolderPath.FullName, fiFastaFile.Name & MSGF_PLUS_INDEX_FILE_INFO_SUFFIX))

			Dim fileSizeTotalKB As Int64 = 0

			If Not fiMSGFPlusIndexFileInfo.Exists Then
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If

			' Read the filenames in the file
			' There should be 3 columns: FileName, FileSize, and FileDateUTC
			' When looking for existing files we only require that the filesize match; FileDateUTC is not used

			Dim dctFilesToCopy As Dictionary(Of String, Int64)
			dctFilesToCopy = New Dictionary(Of String, Int64)

			Using srInFile As StreamReader = New StreamReader(New FileStream(fiMSGFPlusIndexFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

				Dim strLineIn As String
				Dim lstData As List(Of String)

                Do While Not srInFile.EndOfStream
                    strLineIn = srInFile.ReadLine()

                    lstData = strLineIn.Split(ControlChars.Tab).ToList()

                    If lstData.Count >= 3 Then
                        ' Add this file to the list of files to copy
                        Dim intFileSizeBytes As Int64
                        If Int64.TryParse(lstData(1), intFileSizeBytes) Then
                            dctFilesToCopy.Add(lstData(0), intFileSizeBytes)
                            fileSizeTotalKB += CLng(intFileSizeBytes / 1024.0)
                        End If

                    End If
                Loop

			End Using

			Dim blnFilesAreValid As Boolean

			If dctFilesToCopy.Count = 0 Then
				blnFilesAreValid = False
			Else
				' Confirm that each file in dctFilesToCopy exists on the remote server
				' If using a legacy fasta file, then must also confirm that each file is newer than the fasta file that was indexed
				blnFilesAreValid = ValidateFiles(diRemoteIndexFolderPath.FullName, dctFilesToCopy, blnUsingLegacyFasta, fiFastaFile.LastWriteTimeUtc)
			End If

			If Not blnFilesAreValid Then
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If

			If intDebugLevel >= 1 AndAlso fileSizeTotalKB >= 1000 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying existing MSGF+ index files from " & diRemoteIndexFolderPath.FullName)
			End If

			' Copy each file in lstFilesToCopy (overwrite existing files)
			Dim oFileTools As PRISM.Files.clsFileTools
			Dim strManager As String = GetPseudoManagerName()

			Dim filesCopied As Integer = 0
			Dim dtLastStatusUpdate = DateTime.UtcNow

			oFileTools = New PRISM.Files.clsFileTools(strManager, intDebugLevel)

			' Compute the total disk space required
			Dim fileSizeTotalBytes As Int64 = 0

			For Each entry As KeyValuePair(Of String, Int64) In dctFilesToCopy
				Dim fiSourceFile = New FileInfo(Path.Combine(diRemoteIndexFolderPath.FullName, entry.Key))
				fileSizeTotalBytes += fiSourceFile.Length
			Next

			Const DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB = 750

			' Convert fileSizeTotalBytes to MB, but add on a Default_Min_free_Space to assure we'll still have enough free space after copying over the files
			Dim minFreeSpaceMB = CInt(fileSizeTotalBytes / 1024.0 / 1024.0 + DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB)

			diskFreeSpaceBelowThreshold = Not clsGlobal.ValidateFreeDiskSpace("Organism DB directory", fiFastaFile.Directory.FullName, minFreeSpaceMB, clsLogTools.LoggerTypes.LogFile, mErrorMessage)

			If diskFreeSpaceBelowThreshold Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Dim fiRemoteLockFile2 As FileInfo = Nothing
			Dim blnRemoteLockFileCreated As Boolean

			blnRemoteLockFileCreated = CreateRemoteSuffixArrayLockFile(fiFastaFile.Name, fiFastaFile.Directory.FullName, fiRemoteLockFile2, intDebugLevel, sngMaxWaitTimeHours)

			If blnRemoteLockFileCreated Then
				' Lock file successfully created
				' If this manager ended up waiting while another manager was indexing the files or while another manager was copying files locally, 
				' then we should once again check to see if the required files exist

				' Now confirm that each file was successfully copied locally
				blnSuccess = ValidateFiles(fiFastaFile.Directory.FullName, dctFilesToCopy, blnUsingLegacyFasta, fiFastaFile.LastWriteTimeUtc)
				If blnSuccess Then
					' Files now exist
					DeleteLockFile(fiRemoteLockFile2)
					Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
				End If

			End If

			For Each entry As KeyValuePair(Of String, Int64) In dctFilesToCopy

				Dim fiSourceFile = New FileInfo(Path.Combine(diRemoteIndexFolderPath.FullName, entry.Key))

				Dim strTargetFilePath = Path.Combine(fiFastaFile.Directory.FullName, fiSourceFile.Name)
				oFileTools.CopyFileUsingLocks(fiSourceFile, strTargetFilePath, strManager, True)

				filesCopied += 1

				If intDebugLevel >= 1 AndAlso DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 30 Then
					dtLastStatusUpdate = DateTime.UtcNow
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieved " & filesCopied & " / " & dctFilesToCopy.Count & " index files")
				End If

			Next

			' Now confirm that each file was successfully copied locally
			blnSuccess = ValidateFiles(fiFastaFile.Directory.FullName, dctFilesToCopy, blnUsingLegacyFasta, fiFastaFile.LastWriteTimeUtc)

			DeleteLockFile(fiRemoteLockFile2)

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in CopyExistingIndexFilesFromRemote; " & ex.Message)
			blnSuccess = False
		End Try

		If blnSuccess Then
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		Else
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

	End Function

	Protected Function CopyIndexFilesToRemote(
	  ByVal fiFastaFile As FileInfo,
	  ByVal strRemoteIndexFolderPath As String,
	  ByVal intDebugLevel As Integer) As Boolean

		Dim strErrorMessage As String = String.Empty
		Dim strManager As String = GetPseudoManagerName()
		Const createIndexFileForExistingFiles As Boolean = False

		Dim success = CopyIndexFilesToRemote(fiFastaFile, strRemoteIndexFolderPath, intDebugLevel, strManager, createIndexFileForExistingFiles, strErrorMessage)
		If Not success Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strErrorMessage)
		End If

		Return success

	End Function

	''' <summary>
	''' Copies the suffix array files for the specified fasta file to the remote MSGFPlus_Index_File folder share
	''' </summary>
	''' <param name="fiFastaFile"></param>
	''' <param name="remoteIndexFolderPath"></param>
	''' <param name="debugLevel"></param>
	''' <param name="managerName">Manager name (only required because the constructor for PRISM.Files.clsFileTools requires this)</param>
	''' <param name="createIndexFileForExistingFiles">When true, then assumes that the index files were previously copied to remoteIndexFolderPath, and we should simply create the .MSGFPlusIndexFileInfo file for the matching files</param>
	''' <param name="strErrorMessage"></param>
	''' <returns></returns>
	''' <remarks>This function is used both by this class and by the MSGFPlusIndexFileCopier console application</remarks>
	Public Shared Function CopyIndexFilesToRemote(
	  ByVal fiFastaFile As FileInfo,
	  ByVal remoteIndexFolderPath As String,
	  ByVal debugLevel As Integer,
	  ByVal managerName As String,
	  ByVal createIndexFileForExistingFiles As Boolean,
	  <Out()> ByRef strErrorMessage As String) As Boolean

		Dim blnSuccess As Boolean = False
		strErrorMessage = String.Empty

		Try
			Dim diRemoteIndexFolderPath As DirectoryInfo
			diRemoteIndexFolderPath = New DirectoryInfo(remoteIndexFolderPath)

			If Not diRemoteIndexFolderPath.Parent.Exists Then
				strErrorMessage = "MSGF+ index files folder not found: " & diRemoteIndexFolderPath.Parent.FullName
				Return False
			End If

			If Not diRemoteIndexFolderPath.Exists Then
				diRemoteIndexFolderPath.Create()
			End If

			If createIndexFileForExistingFiles Then
				Dim remoteFastaPath = Path.Combine(remoteIndexFolderPath, fiFastaFile.Name)
				fiFastaFile = New FileInfo(remoteFastaPath)
			End If

			Dim dctFilesToCopy As Dictionary(Of String, Int64)
			dctFilesToCopy = New Dictionary(Of String, Int64)

			Dim lstFileInfo As List(Of String)
			lstFileInfo = New List(Of String)

			' Find the index files for fiFastaFile
			For Each fiSourceFile As FileInfo In fiFastaFile.Directory.GetFiles(Path.GetFileNameWithoutExtension(fiFastaFile.Name) & ".*")
				If fiSourceFile.FullName <> fiFastaFile.FullName Then
					If fiSourceFile.Extension <> ".hashcheck" AndAlso fiSourceFile.Extension <> ".MSGFPlusIndexFileInfo" Then
						dctFilesToCopy.Add(fiSourceFile.Name, fiSourceFile.Length)
						lstFileInfo.Add(fiSourceFile.Name & ControlChars.Tab & fiSourceFile.Length & ControlChars.Tab & fiSourceFile.LastWriteTimeUtc.ToString("yyyy-MM-dd hh:mm:ss tt"))
					End If
				End If
			Next

			If createIndexFileForExistingFiles Then
				blnSuccess = True
			Else
				' Copy up each file
				Dim oFileTools As PRISM.Files.clsFileTools

				oFileTools = New PRISM.Files.clsFileTools(managerName, debugLevel)

				For Each entry As KeyValuePair(Of String, Int64) In dctFilesToCopy
					Dim strSourceFilePath As String
					Dim strTargetFilePath As String

					strSourceFilePath = Path.Combine(fiFastaFile.Directory.FullName, entry.Key)
					strTargetFilePath = Path.Combine(diRemoteIndexFolderPath.FullName, entry.Key)

					blnSuccess = oFileTools.CopyFileUsingLocks(strSourceFilePath, strTargetFilePath, managerName, True)
					If Not blnSuccess Then
						strErrorMessage = "CopyFileUsingLocks returned false copying to " & strTargetFilePath
						Exit For
					End If

				Next
			End If

			If blnSuccess Then

				' Create the .MSGFPlusIndexFileInfo file for this fasta file
				Dim fiMSGFPlusIndexFileInfo As FileInfo
				fiMSGFPlusIndexFileInfo = New FileInfo(Path.Combine(diRemoteIndexFolderPath.FullName, fiFastaFile.Name & MSGF_PLUS_INDEX_FILE_INFO_SUFFIX))

				Using swOutFile As StreamWriter = New StreamWriter(New FileStream(fiMSGFPlusIndexFileInfo.FullName, FileMode.Create, FileAccess.Write, FileShare.Read))

					For Each entry As String In lstFileInfo
						swOutFile.WriteLine(entry)
					Next

				End Using

				blnSuccess = True

			End If

		Catch ex As Exception
			strErrorMessage = "Exception in CopyIndexFilesToRemote; " & ex.Message
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Convert .Fasta file to indexed DB files compatible with MSGFPlus
	''' Will copy the files from strMSGFPlusIndexFilesFolderPathBase if they exist
	''' </summary>
	''' <param name="strLogfileDir"></param>
	''' <param name="intDebugLevel"></param>
	''' <param name="JobNum"></param>
	''' <param name="javaProgLoc"></param>
	''' <param name="msgfDbProgLoc"></param>
	''' <param name="strFASTAFilePath">Input/output parameter; will get updated if running Legacy MSGFDB</param>
	''' <param name="blnFastaFileIsDecoy">When True, then only creates the forward-based index files.  When False, then creates both the forward and reverse index files</param>
	''' <param name="strMSGFPlusIndexFilesFolderPathBase">Folder path from which to copy (or store) the index files</param>
	''' <param name="strMSGFPlusIndexFilesFolderPathLegacyDB">Folder path from which to copy (or store) the index files for Legacy DBs (.fasta files not created from the protein sequences database)</param>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Function CreateSuffixArrayFiles(
	  ByVal strLogFileDir As String,
	  ByVal intDebugLevel As Integer,
	  ByVal JobNum As String,
	  ByVal javaProgLoc As String,
	  ByVal msgfDbProgLoc As String,
	  ByRef strFASTAFilePath As String,
	  ByVal blnFastaFileIsDecoy As Boolean,
	  ByVal strMSGFPlusIndexFilesFolderPathBase As String,
	  ByVal strMSGFPlusIndexFilesFolderPathLegacyDB As String,
	  ByVal udtHPCOptions As clsAnalysisResources.udtHPCOptionsType) As IJobParams.CloseOutType

		Const MAX_WAITTIME_HOURS As Single = 1.0

		Dim strOutputNameBase As String

		Dim fiLockFile As FileInfo
		Dim dbSarrayFilename As String

		Dim sngMaxWaitTimeHours As Single = MAX_WAITTIME_HOURS

		Dim blnMSGFPlus As Boolean
		Dim strCurrentTask As String = "Initializing"
		Dim eResult As IJobParams.CloseOutType

		Try
			mErrorMessage = String.Empty

			If intDebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsCreateMSGFDBSuffixArrayFiles.CreateIndexedDbFiles(): Enter")
			End If

			Dim fiFastaFile As FileInfo
			fiFastaFile = New FileInfo(strFASTAFilePath)

			blnMSGFPlus = IsMSGFPlus(msgfDbProgLoc)
			If Not blnMSGFPlus Then
				' Running legacy MS-GFDB
				Throw New Exception("Legacy MS-GFDB is no longer supported")
			End If

			' Protein collection files will start with ID_ then have at least 6 integers, then an alphanumeric hash string, for example ID_004208_295531A4.fasta
			' If the filename does not match that pattern, then we're using a legacy fasta file
			Dim reProtectionCollectionFasta = New Text.RegularExpressions.Regex("ID_\d{6,}_[0-9a-z]+\.fasta", Text.RegularExpressions.RegexOptions.IgnoreCase)
			Dim blnUsingLegacyFasta = Not reProtectionCollectionFasta.IsMatch(fiFastaFile.Name)

			'  Look for existing suffix array files 
			strOutputNameBase = Path.GetFileNameWithoutExtension(fiFastaFile.Name)

			fiLockFile = New FileInfo(Path.Combine(fiFastaFile.DirectoryName, strOutputNameBase & "_csarr.lock"))
			dbSarrayFilename = Path.Combine(fiFastaFile.DirectoryName, strOutputNameBase & ".csarr")

			If udtHPCOptions.UsingHPC Then
				' Increase the maximum wait time to 24 hours; useful in case another manager has created a BuildSA job, and that job is stuck in the queue
				sngMaxWaitTimeHours = 24
			End If

			' Check to see if another Analysis Manager is already creating the indexed DB files
			strCurrentTask = "Looking for lock file " & fiLockFile.FullName
			WaitForExistingLockfile(fiLockFile, intDebugLevel, sngMaxWaitTimeHours)

			' Validate that all of the expected files exist
			' If any are missing, then need to repeat the call to "BuildSA"
			Dim blnReindexingRequired As Boolean = False

			strCurrentTask = "Validating that expected files exist"
			If blnMSGFPlus Then
				' Check for any FastaFileName.revConcat.* files
				' If they exist, delete them, since they are for legacy MSGFDB

				Dim fiLegacyIndexedFiles() As FileInfo
				fiLegacyIndexedFiles = fiFastaFile.Directory.GetFiles(strOutputNameBase & ".revConcat.*")

				If fiLegacyIndexedFiles.Length > 0 Then
					blnReindexingRequired = True

					For intIndex As Integer = 0 To fiLegacyIndexedFiles.Length - 1
						strCurrentTask = "Deleting indexed file created by legacy MSGFDB: " & fiLegacyIndexedFiles(intIndex).FullName
						If intDebugLevel >= 1 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strCurrentTask)
						End If
						fiLegacyIndexedFiles(intIndex).Delete()
					Next

				Else
					' Open the FastaFileName.canno file and read the first two lines
					' If there is a number on the first line but the second line starts with the letter A, then this file was created with the legacy MSGFDB
					Dim fiCAnnoFile As FileInfo
					fiCAnnoFile = New FileInfo(Path.Combine(fiFastaFile.DirectoryName, strOutputNameBase & ".canno"))
					If fiCAnnoFile.Exists Then

						strCurrentTask = "Examining first two lines of " & fiCAnnoFile.FullName
						Using srCannoFile As StreamReader = New StreamReader(New FileStream(fiCAnnoFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))

                            If Not srCannoFile.EndOfStream Then
                                Dim strLine1 As String
                                Dim strLine2 As String
                                Dim intLine1Value As Integer

                                strLine1 = srCannoFile.ReadLine()

                                If Not srCannoFile.EndOfStream Then
                                    strLine2 = srCannoFile.ReadLine()

                                    If Integer.TryParse(strLine1, intLine1Value) Then
                                        If Char.IsLetter(strLine2.Chars(0)) Then
                                            strCurrentTask = "Legacy MSGFDB indexed file found (" & fiCAnnoFile.Name & "); re-indexing"
                                            If intDebugLevel >= 1 Then
                                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strCurrentTask)
                                            End If
                                            blnReindexingRequired = True
                                        End If
                                    End If
                                End If
                            End If
						End Using
					End If

				End If
			End If


			' This dictionary contains file suffixes to look for
			' Keys will be "True" if the file exists and false if it does not exist
			Dim lstFilesToFind = New List(Of String)

			If Not blnReindexingRequired Then

				Dim strExistingFiles As String = String.Empty
				Dim strMissingFiles As String = String.Empty

				strCurrentTask = "Validating that expected files exist"
				Dim lstExistingFiles = FindExistingSuffixArrayFiles(blnFastaFileIsDecoy, blnMSGFPlus, strOutputNameBase, fiFastaFile.DirectoryName, lstFilesToFind, strExistingFiles, strMissingFiles)

				If lstExistingFiles.Count < lstFilesToFind.Count Then
					blnReindexingRequired = True

					strCurrentTask = "Some files are missing: " & lstExistingFiles.Count & " vs. " & lstFilesToFind.Count
					If lstExistingFiles.Count > 0 Then
						If intDebugLevel >= 1 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Indexing of " & fiFastaFile.Name & " was incomplete (found " & lstExistingFiles.Count & " out of " & lstFilesToFind.Count & " index files)")
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... existing files: " & strExistingFiles)
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... missing files: " & strMissingFiles)
						End If
					End If
				ElseIf blnUsingLegacyFasta Then

					' Make sure all of the index files have a file modification date newer than the fasta file
					' We only do this for legacy fasta files, since their file modification date will be the same on all pubs
					' We can't do this for programatically generated fasta files (that use protein collections) 
					'   since their modification date will be the time that the file was created

					blnReindexingRequired = False

					For Each fiIndexFile In lstExistingFiles
						If fiIndexFile.LastWriteTimeUtc < fiFastaFile.LastWriteTimeUtc.AddSeconds(-0.1) Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Index file is older than the fasta file; " &
							  fiIndexFile.FullName & " modified " &
							  fiIndexFile.LastWriteTimeUtc.ToLocalTime().ToString("yyyy-MM-dd hh:mm:ss tt") & " vs. " &
							  fiFastaFile.LastWriteTimeUtc.ToLocalTime().ToString("yyyy-MM-dd hh:mm:ss tt"))

							blnReindexingRequired = True
							Exit For
						End If
					Next
				End If

			End If

			If blnReindexingRequired Then

				' Index files are missing or out of date
				' Copy them from strMSGFPlusIndexFilesFolderPathBase or strMSGFPlusIndexFilesFolderPathLegacyDB if possible
				' Otherwise, create new index files

				Dim strRemoteIndexFolderPath As String
				strRemoteIndexFolderPath = DetermineRemoteMSGFPlusIndexFilesFolderPath(fiFastaFile.Name, strMSGFPlusIndexFilesFolderPathBase, strMSGFPlusIndexFilesFolderPathLegacyDB)

				Dim blnCheckForLockFile As Boolean
				Dim diskFreeSpaceBelowThreshold As Boolean = False

				blnCheckForLockFile = True
				eResult = CopyExistingIndexFilesFromRemote(fiFastaFile, blnUsingLegacyFasta, strRemoteIndexFolderPath, blnCheckForLockFile, intDebugLevel, sngMaxWaitTimeHours, diskFreeSpaceBelowThreshold)

				If diskFreeSpaceBelowThreshold Then
					' Not enough free disk space; abort
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
					' Files did not exist or were out of date, or an error occurred while copying them

					' Create a remote lock file

					Dim fiRemoteLockFile As FileInfo = Nothing
					Dim blnRemoteLockFileCreated As Boolean

					strCurrentTask = "Create the remote lock file"
					blnRemoteLockFileCreated = CreateRemoteSuffixArrayLockFile(fiFastaFile.Name, strRemoteIndexFolderPath, fiRemoteLockFile, intDebugLevel, sngMaxWaitTimeHours)

					If blnRemoteLockFileCreated Then
						' Lock file successfully created
						' If this manager ended up waiting while another manager was indexing the files, then we should once again try to copy the files locally

						blnCheckForLockFile = False
						eResult = CopyExistingIndexFilesFromRemote(fiFastaFile, blnUsingLegacyFasta, strRemoteIndexFolderPath, blnCheckForLockFile, intDebugLevel, sngMaxWaitTimeHours, diskFreeSpaceBelowThreshold)

						If eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
							' Existing files were copied; this manager does not need to re-create them
							blnReindexingRequired = False
						End If

						If diskFreeSpaceBelowThreshold Then
							' Not enough free disk space; abort
							Return IJobParams.CloseOutType.CLOSEOUT_FAILED
						End If
					End If

					If blnReindexingRequired Then

						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running BuildSA to index " & fiFastaFile.Name)

						eResult = CreateSuffixArrayFilesWork(
						  strLogFileDir, intDebugLevel, JobNum,
						  fiFastaFile, fiLockFile,
						  javaProgLoc, msgfDbProgLoc,
						  blnFastaFileIsDecoy, blnMSGFPlus,
						  dbSarrayFilename,
						  udtHPCOptions)

						If blnRemoteLockFileCreated AndAlso eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying index files to " & strRemoteIndexFolderPath)
							CopyIndexFilesToRemote(fiFastaFile, strRemoteIndexFolderPath, intDebugLevel)
						End If

					End If

					If blnRemoteLockFileCreated Then
						' Delete the remote lock file
						DeleteLockFile(fiRemoteLockFile)
					End If

				End If
			Else
				eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS
			End If

		Catch ex As Exception
			mErrorMessage = "Exception in .CreateIndexedDbFiles"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage & "; " & strCurrentTask & "; " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		End Try

		Return eResult

	End Function

	Protected Function CreateSuffixArrayFilesWork(
	  ByVal strLogFileDir As String,
	  ByVal intDebugLevel As Integer,
	  ByVal JobNum As String,
	  ByVal fiFastaFile As FileInfo,
	  ByVal fiLockFile As FileInfo,
	  ByVal JavaProgLoc As String,
	  ByVal msgfDbProgLoc As String,
	  ByVal blnFastaFileIsDecoy As Boolean,
	  ByVal blnMSGFPlus As Boolean,
	  ByVal dbSarrayFilename As String,
	  ByVal udtHPCOptions As clsAnalysisResources.udtHPCOptionsType) As IJobParams.CloseOutType

		Dim strCurrentTask As String = String.Empty

		Try

			' Try to create the index files for fasta file strDBFileNameInput
			strCurrentTask = "Look for java.exe and .jar file"

			' Verify that Java exists
			If Not File.Exists(JavaProgLoc) Then
				mErrorMessage = "Cannot find Java program file"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage & ": " & JavaProgLoc)
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If

			' Verify that the MSGFDB.Jar or MSGFPlus.jar file exists
			If Not File.Exists(msgfDbProgLoc) Then
				mErrorMessage = "Cannot find " + Path.GetFileName(msgfDbProgLoc) & " file"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage & ": " & msgfDbProgLoc)
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If


			' Determine the amount of ram to reserve for BuildSA
			' Examine the size of the .Fasta file to determine how much ram to reserve
			Dim intJavaMemorySizeMB As Integer

			Dim intFastaFileSizeMB As Integer
			intFastaFileSizeMB = CInt(fiFastaFile.Length / 1024.0 / 1024.0)

			If intFastaFileSizeMB <= 125 Then
				intJavaMemorySizeMB = 4000
			ElseIf intFastaFileSizeMB <= 250 Then
				intJavaMemorySizeMB = 6000
			ElseIf intFastaFileSizeMB <= 375 Then
				intJavaMemorySizeMB = 8000
			Else
				intJavaMemorySizeMB = 12000
			End If

			If udtHPCOptions.UsingHPC Then
				intJavaMemorySizeMB = udtHPCOptions.MinimumMemoryMB
			Else

				strCurrentTask = "Verify free memory"

				' Make sure the machine has enough free memory to run BuildSA
				If Not clsAnalysisResources.ValidateFreeMemorySize(intJavaMemorySizeMB, "BuildSA", False) Then
					mErrorMessage = "Cannot run BuildSA since less than " & intJavaMemorySizeMB & " MB of free memory"
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

			End If

			' Create a lock file
			If intDebugLevel >= 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating lock file: " & fiLockFile.FullName)
			End If

			' Delay between 2 and 5 seconds
			Dim oRandom = New Random()
			Threading.Thread.Sleep(oRandom.Next(2, 5) * 1000)

			' Check one more time for a lock file
			' If it exists, then another manager just created it and we should abort
			strCurrentTask = "Look for the lock file one last time"
			fiLockFile.Refresh()
			If fiLockFile.Exists Then
				If intDebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Warning: new lock file found: " & fiLockFile.FullName & "; aborting")
					Return IJobParams.CloseOutType.CLOSEOUT_NO_FAS_FILES
				End If
			End If

			' Create a lock file in the folder that the index files will be created
			Dim success As Boolean
			strCurrentTask = "Create the local lock file: " & fiLockFile.FullName
			success = CreateLockFile(fiLockFile.FullName)
			If Not success Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Delete any existing index files (BuildSA throws an error if they exist)
			strCurrentTask = "Delete any existing files"

			Dim strOutputNameBase As String = Path.GetFileNameWithoutExtension(fiFastaFile.Name)

			Dim lstExistingFiles = FindExistingSuffixArrayFiles(blnFastaFileIsDecoy, blnMSGFPlus, strOutputNameBase, fiFastaFile.DirectoryName, New List(Of String), String.Empty, String.Empty)

			For Each fiIndexFileToDelete In lstExistingFiles
				If fiIndexFileToDelete.Exists Then
					fiIndexFileToDelete.Delete()
				End If
			Next

			If intDebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating Suffix Array database file: " & dbSarrayFilename)
			End If

			'Set up and execute a program runner to invoke BuildSA (which is in MSGFDB.jar or MSGFPlus.jar)          
			strCurrentTask = "Construct BuildSA command line"
			Dim CmdStr As String
			CmdStr = " -Xmx" & intJavaMemorySizeMB.ToString & "M -cp " & msgfDbProgLoc

			If blnMSGFPlus Then
				CmdStr &= " edu.ucsd.msjava.msdbsearch.BuildSA -d " & fiFastaFile.FullName
			Else
				CmdStr &= " msdbsearch.BuildSA -d " & fiFastaFile.FullName
			End If

			If blnFastaFileIsDecoy Then
				CmdStr &= " -tda 0"
			Else
				CmdStr &= " -tda 2"
			End If

			If intDebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, JavaProgLoc & " " & CmdStr)
			End If

			If udtHPCOptions.UsingHPC Then
				Dim jobName As String = "BuildSA_" & fiFastaFile.Name
				Const taskName = "BuildSA"

				Dim buildSAJobInfo = New HPC_Connector.JobToHPC(udtHPCOptions.HeadNode, jobName, taskName)

				buildSAJobInfo.JobParameters.PriorityLevel = HPC_Connector.PriorityLevel.Normal
				buildSAJobInfo.JobParameters.TemplateName = "DMS"		 ' If using 32 cores, could use Template "Single"
				buildSAJobInfo.JobParameters.ProjectName = "DMS"

				' April 2014 note: If using picfs.pnl.gov then we must reserve an entire node due to file system issues of the Windows Nodes talking to the Isilon file system
				' Furthermore, we must set ".isExclusive" to True
				' Note that each node has two sockets

				'buildSAJobInfo.JobParameters.TargetHardwareUnitType = HPC_Connector.HardwareUnitType.Socket
				buildSAJobInfo.JobParameters.TargetHardwareUnitType = HPC_Connector.HardwareUnitType.Node
				buildSAJobInfo.JobParameters.isExclusive = True

				' If requesting a socket or a node, there is no need to set the number of cores
				' buildSAJobInfo.JobParameters.MinNumberOfCores = 0
				' buildSAJobInfo.JobParameters.MaxNumberOfCores = 0

				' Make a batch file that will run the java program, then issue a Ping command with a delay, which will allow the file system to release the file handles
				Dim batchFilePath = clsAnalysisToolRunnerMSGFDB.MakeHPCBatchFile(udtHPCOptions.WorkDirPath, "HPC_SuffixAray_Task.bat", JavaProgLoc & " " & CmdStr)

				buildSAJobInfo.TaskParameters.CommandLine = batchFilePath
				buildSAJobInfo.TaskParameters.WorkDirectory = udtHPCOptions.WorkDirPath
				buildSAJobInfo.TaskParameters.StdOutFilePath = Path.Combine(udtHPCOptions.WorkDirPath, "MSGFDB_BuildSA_ConsoleOutput.txt")
				buildSAJobInfo.TaskParameters.TaskTypeOption = HPC_Connector.HPCTaskType.Basic
				buildSAJobInfo.TaskParameters.FailJobOnFailure = True

				If String.IsNullOrEmpty(mPICHPCUser) Then
					mComputeCluster = New HPC_Submit.WindowsHPC2012()
				Else
					mComputeCluster = New HPC_Submit.WindowsHPC2012(mPICHPCUser, clsGlobal.DecodePassword(mPICHPCPassword))
				End If

				Dim jobID = mComputeCluster.Send(buildSAJobInfo)

				If jobID <= 0 Then
					mErrorMessage = "BuildSA Job was not created in HPC: " & mComputeCluster.ErrorMessage
					DeleteLockFile(fiLockFile)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				If mComputeCluster.Scheduler Is Nothing Then
					mErrorMessage = "Error: HPC Scheduler is null for BuildSA Job"
					DeleteLockFile(fiLockFile)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				Dim buildSAJob = mComputeCluster.Scheduler.OpenJob(jobID)

				success = mComputeCluster.MonitorJob(buildSAJob)
				If Not success Then
					mErrorMessage = "HPC Job Monitor returned false: " & mComputeCluster.ErrorMessage
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage)
				End If

				Try
					File.Delete(batchFilePath)
				Catch ex As Exception
					' Ignore errors here
				End Try

			Else

				Dim objBuildSA As clsRunDosProgram
				objBuildSA = New clsRunDosProgram(fiFastaFile.DirectoryName)

				With objBuildSA
					.CreateNoWindow = True
					.CacheStandardOutput = True
					.EchoOutputToConsole = True

					.WriteConsoleOutputToFile = True
					.ConsoleOutputFilePath = Path.Combine(strLogFileDir, "MSGFDB_BuildSA_ConsoleOutput.txt")
				End With

				strCurrentTask = "Run BuildSA using " & CmdStr
				success = objBuildSA.RunProgram(JavaProgLoc, CmdStr, "BuildSA", True)

			End If

			If Not success Then
				mErrorMessage = "Error running BuildSA with " & Path.GetFileName(msgfDbProgLoc) & " for " & fiFastaFile.Name
				If udtHPCOptions.UsingHPC Then
					mErrorMessage &= " using HPC"
				End If
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mErrorMessage & ": " & JobNum)
				DeleteLockFile(fiLockFile)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			Else
				If intDebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Created suffix array files for " & fiFastaFile.Name)
				End If
			End If

			If intDebugLevel >= 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting lock file: " & fiLockFile.FullName)
			End If

			' Delete the lock file
			strCurrentTask = "Delete the lock file"
			DeleteLockFile(fiLockFile)

		Catch ex As Exception
			mErrorMessage = "Exception in .CreateSuffixArrayFilesWork"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage & "; " & strCurrentTask & "; " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Creates a lock file
	''' </summary>
	''' <returns>True if success; false if failure</returns>
	Protected Function CreateLockFile(ByVal strLockFilePath As String) As Boolean

		Try
			Using sw As StreamWriter = New StreamWriter(strLockFilePath)
				' Add Date and time to the file.
				sw.WriteLine(DateTime.Now)
			End Using

		Catch ex As Exception
			mErrorMessage = "Error creating lock file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsCreateMSGFDBSuffixArrayFiles.CreateLockFile, " & mErrorMessage & ": " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	Protected Function CreateRemoteSuffixArrayLockFile(
	  ByVal strFastaFileName As String,
	  ByVal strRemoteIndexFolderPath As String,
	  ByRef fiRemoteLockFile As FileInfo,
	  ByVal intDebugLevel As Integer,
	  ByVal sngMaxWaitTimeHours As Single) As Boolean

		' ReSharper disable once RedundantAssignment
		Dim strCurrentTask As String = "Initializing"

		' ReSharper disable once RedundantAssignment
		strCurrentTask = "Looking for folder " & strRemoteIndexFolderPath

		Dim diRemoteIndexFolderPath As DirectoryInfo
		diRemoteIndexFolderPath = New DirectoryInfo(strRemoteIndexFolderPath)

		If Not diRemoteIndexFolderPath.Parent.Exists Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot read/write MSGF+ index files from remote share; folder not found; " & diRemoteIndexFolderPath.FullName)
			Return False
		End If

		fiRemoteLockFile = New FileInfo(Path.Combine(diRemoteIndexFolderPath.FullName, strFastaFileName & MSGF_PLUS_INDEX_FILE_INFO_SUFFIX & ".lock"))

		strCurrentTask = "Looking for lock file " & fiRemoteLockFile.FullName
		WaitForExistingLockfile(fiRemoteLockFile, intDebugLevel, sngMaxWaitTimeHours)

		Try

			If Not diRemoteIndexFolderPath.Exists Then
				diRemoteIndexFolderPath.Create()
			End If

			' Create the remote lock file
			If Not CreateLockFile(fiRemoteLockFile.FullName) Then
				Return False
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception creating remote MSGF+ suffix array lock file at " & diRemoteIndexFolderPath.FullName & "; " & strCurrentTask & "; " & ex.Message)
			Return False
		End Try

		Return True

	End Function


	Protected Sub DeleteLockFile(ByVal fiLockFile As FileInfo)
		Try
			fiLockFile.Refresh()
			If fiLockFile.Exists Then
				fiLockFile.Delete()
			End If
		Catch ex As Exception
			' Ignore errors here
		End Try
	End Sub

	Protected Function DetermineRemoteMSGFPlusIndexFilesFolderPath(
	  ByVal strFastaFileName As String,
	  ByVal strMSGFPlusIndexFilesFolderPathBase As String,
	  ByVal strMSGFPlusIndexFilesFolderPathLegacyDB As String) As String

		Dim reExtractNum As Text.RegularExpressions.Regex = New Text.RegularExpressions.Regex("^ID_(\d+)", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
		Dim reMatch As Text.RegularExpressions.Match

		Dim strRemoteIndexFolderPath As String = String.Empty

		' DMS-generated fasta files will have a name of the form ID_003949_3D6802EE.fasta
		' Parse out the number (003949 in this case)
		reMatch = reExtractNum.Match(strFastaFileName)
		If reMatch.Success Then
			Dim intGeneratedFastaFileNumber As Integer

			If Integer.TryParse(reMatch.Groups.Item(1).Value, intGeneratedFastaFileNumber) Then
				' Round down to the nearest 1000
				' Thus, 003949 will round to 3000
				Dim strFolderName = (Math.Floor(intGeneratedFastaFileNumber / 1000.0) * 1000).ToString("0")
				strRemoteIndexFolderPath = Path.Combine(strMSGFPlusIndexFilesFolderPathBase, strFolderName)
			End If
		End If

		If String.IsNullOrEmpty(strRemoteIndexFolderPath) Then
			strRemoteIndexFolderPath = Path.Combine(strMSGFPlusIndexFilesFolderPathLegacyDB, "Other")
		End If

		Return strRemoteIndexFolderPath

	End Function

	''' <summary>
	''' Constructs a list of suffix array files that should exist
	''' Looks for each of those files
	''' </summary>
	''' <param name="blnFastaFileIsDecoy"></param>
	''' <param name="blnMSGFPlus"></param>
	''' <param name="strOutputNameBase"></param>
	''' <param name="strFolderPathToSearch"></param>
	''' <param name="lstFilesToFind">Output param: list of files that should exist</param>
	''' <returns>A list of the files that currently exist</returns>
	''' <remarks></remarks>
	Protected Function FindExistingSuffixArrayFiles(
	  ByVal blnFastaFileIsDecoy As Boolean,
	  ByVal blnMSGFPlus As Boolean,
	  ByVal strOutputNameBase As String,
	  ByVal strFolderPathToSearch As String,
	  ByRef lstFilesToFind As List(Of String)) As List(Of FileInfo)

		Dim strExistingFiles As String = String.Empty
		Dim strMissingFiles As String = String.Empty

		Return FindExistingSuffixArrayFiles(blnFastaFileIsDecoy, blnMSGFPlus, strOutputNameBase, strFolderPathToSearch, lstFilesToFind, strExistingFiles, strMissingFiles)

	End Function

	''' <summary>
	''' Constructs a list of suffix array files that should exist
	''' Looks for each of those files
	''' </summary>
	''' <param name="blnFastaFileIsDecoy"></param>
	''' <param name="blnMSGFPlus"></param>
	''' <param name="strOutputNameBase"></param>
	''' <param name="strFolderPathToSearch"></param>
	''' <param name="lstFilesToFind">Output param: list of files that should exist</param>
	''' <param name="strExistingFiles">Output param: semicolon separated list of existing files</param>
	''' <param name="strMissingFiles">Output param: semicolon separated list of missing files</param>
	''' <returns>A list of the files that currently exist</returns>
	''' <remarks></remarks>
	Protected Function FindExistingSuffixArrayFiles(
	  ByVal blnFastaFileIsDecoy As Boolean,
	  ByVal blnMSGFPlus As Boolean,
	  ByVal strOutputNameBase As String,
	  ByVal strFolderPathToSearch As String,
	  ByRef lstFilesToFind As List(Of String),
	  ByRef strExistingFiles As String,
	  ByRef strMissingFiles As String) As List(Of FileInfo)

		Dim lstExistingFiles As List(Of FileInfo)
		lstExistingFiles = New List(Of FileInfo)

		If lstFilesToFind Is Nothing Then
			lstFilesToFind = New List(Of String)
		Else
			lstFilesToFind.Clear()
		End If

		strExistingFiles = String.Empty
		strMissingFiles = String.Empty

		' Old suffixes (used prior to August 2011)
		'lstFilesToFind.Add(".revConcat.fasta")
		'lstFilesToFind.Add(".seq")
		'lstFilesToFind.Add(".seqanno")
		'lstFilesToFind.Add(".revConcat.seq")
		'lstFilesToFind.Add(".revConcat.seqanno")
		'lstFilesToFind.Add(".sarray")
		'lstFilesToFind.Add(".revConcat.sarray")

		' Suffixes for MSGFDB (effective 8/22/2011) and MSGF+ 
		lstFilesToFind.Add(".canno")
		lstFilesToFind.Add(".cnlcp")
		lstFilesToFind.Add(".csarr")
		lstFilesToFind.Add(".cseq")

		' Note: Suffixes for MSPathFinder
		' lstFilesToFind.Add(".icanno")
		' lstFilesToFind.Add(".icplcp")
		' lstFilesToFind.Add(".icseq")

		If Not blnFastaFileIsDecoy Then
			If blnMSGFPlus Then
				lstFilesToFind.Add(".revCat.canno")
				lstFilesToFind.Add(".revCat.cnlcp")
				lstFilesToFind.Add(".revCat.csarr")
				lstFilesToFind.Add(".revCat.cseq")
				lstFilesToFind.Add(".revCat.fasta")
			Else
				lstFilesToFind.Add(".revConcat.canno")
				lstFilesToFind.Add(".revConcat.cnlcp")
				lstFilesToFind.Add(".revConcat.csarr")
				lstFilesToFind.Add(".revConcat.cseq")
				lstFilesToFind.Add(".revConcat.fasta")
			End If
		End If

		For Each strSuffix In lstFilesToFind

			Dim strFileNameToFind As String = strOutputNameBase & strSuffix

			Dim fiFileToFind = New FileInfo(Path.Combine(strFolderPathToSearch, strFileNameToFind))

			If fiFileToFind.Exists() Then
				lstExistingFiles.Add(fiFileToFind)
				strExistingFiles = clsGlobal.AppendToComment(strExistingFiles, strFileNameToFind)
			Else
				strMissingFiles = clsGlobal.AppendToComment(strMissingFiles, strFileNameToFind)
			End If
		Next

		Return lstExistingFiles
	End Function

	Protected Function GetPseudoManagerName() As String

		Dim strMgrName As String
		strMgrName = mMgrName & "_CreateMSGFDBSuffixArrayFiles"

		Return strMgrName
	End Function

	Public Function IsMSGFPlus(ByVal MSGFDBJarFilePath As String) As Boolean
		Const MSGFDB_JAR_NAME As String = "MSGFDB.jar"

		Dim fiJarFile As FileInfo
		fiJarFile = New FileInfo(MSGFDBJarFilePath)

		If String.Compare(fiJarFile.Name, MSGFDB_JAR_NAME, True) = 0 Then
			' Not MSGF+
			Return False
		Else
			' Using MSGF+
			Return True
		End If

	End Function

	''' <summary>
	''' Verifies that each of the files specified by dctFilesToCopy exists at strFolderPathToCheck and has the correct file size
	''' </summary>
	''' <param name="strFolderPathToCheck">folder to check</param>
	''' <param name="dctFilesToCopy">Dictionary with filenames and file sizes</param>
	''' <param name="blnUsingLegacyFasta"></param>
	''' <param name="dtMinWriteTimeThresholdUTC"></param>
	''' <returns>True if all files are found and are the right size</returns>
	''' <remarks></remarks>
	Protected Function ValidateFiles(
	  ByVal strFolderPathToCheck As String,
	  ByVal dctFilesToCopy As Dictionary(Of String, Int64),
	  ByVal blnUsingLegacyFasta As Boolean,
	  ByVal dtMinWriteTimeThresholdUTC As DateTime) As Boolean

		For Each entry As KeyValuePair(Of String, Int64) In dctFilesToCopy
			Dim fiSourceFile As FileInfo
			fiSourceFile = New FileInfo(Path.Combine(strFolderPathToCheck, entry.Key))

			If Not fiSourceFile.Exists Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Remote MSGF+ index file not found: " & fiSourceFile.FullName)
				Return False
			ElseIf fiSourceFile.Length <> entry.Value Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Remote MSGF+ index file is not the expected size: " & fiSourceFile.FullName & " should be " & entry.Value & " bytes but is actually " & fiSourceFile.Length & " bytes")
				Return False
			ElseIf blnUsingLegacyFasta Then
				' Require that the index files be newer than the fasta file
				If fiSourceFile.LastWriteTimeUtc < dtMinWriteTimeThresholdUTC.AddSeconds(-0.1) Then

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Index file is older than the fasta file; " &
					  fiSourceFile.FullName & " modified " &
					  fiSourceFile.LastWriteTimeUtc.ToLocalTime().ToString("yyyy-MM-dd hh:mm:ss tt") & " vs. " &
					  dtMinWriteTimeThresholdUTC.ToLocalTime().ToString("yyyy-MM-dd hh:mm:ss tt"))

					Return False
				End If
			End If
		Next

		Return True

	End Function

	Protected Sub WaitForExistingLockfile(ByVal fiLockFile As FileInfo, ByVal intDebugLevel As Integer, ByVal sngMaxWaitTimeHours As Single)

		' Check to see if another Analysis Manager is already creating the indexed DB files
		If fiLockFile.Exists AndAlso DateTime.UtcNow.Subtract(fiLockFile.LastWriteTimeUtc).TotalMinutes >= 60 Then
			' Lock file is over 60 minutes old; delete it
			If intDebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Lock file is over 60 minutes old (created " & fiLockFile.LastWriteTime.ToString() & "); deleting " & fiLockFile.FullName)
			End If
			DeleteLockFile(fiLockFile)

		ElseIf fiLockFile.Exists Then

			If intDebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Lock file found: " & fiLockFile.FullName & "; waiting for file to be removed by other manager generating suffix array files")
			End If

			' Lock file found; wait up to sngMaxWaitTimeHours hours
			Dim blnStaleFile As Boolean = False
			Do While fiLockFile.Exists
				' Sleep for 2 seconds
				Threading.Thread.Sleep(2000)

				If DateTime.UtcNow.Subtract(fiLockFile.CreationTimeUtc).TotalHours >= sngMaxWaitTimeHours Then
					blnStaleFile = True
					Exit Do
				Else
					fiLockFile.Refresh()
				End If
			Loop

			'If the duration time has exceeded sngMaxWaitTimeHours, then delete the lock file and try again with this manager
			If blnStaleFile Then
				Dim strLogMessage As String
				strLogMessage = "Waited over " & sngMaxWaitTimeHours.ToString("0.0") & " hour(s) for lock file to be deleted, but it is still present; deleting the file now and continuing: " & fiLockFile.FullName
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strLogMessage)
				DeleteLockFile(fiLockFile)
			End If

		End If

	End Sub

#Region "Event Handlers"
	Private Sub mComputeCluster_ErrorEvent(sender As Object, e As HPC_Submit.MessageEventArgs) Handles mComputeCluster.ErrorEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, e.Message)
	End Sub

	Private Sub mComputeCluster_MessageEvent(sender As Object, e As HPC_Submit.MessageEventArgs) Handles mComputeCluster.MessageEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, e.Message)
	End Sub

	Private Sub mComputeCluster_ProgressEvent(sender As Object, e As HPC_Submit.ProgressEventArgs) Handles mComputeCluster.ProgressEvent
		Static dtLastStatusUpdate As DateTime = DateTime.UtcNow

		If DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 60 Then
			dtLastStatusUpdate = DateTime.UtcNow
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running BuildSA with HPC, " & (e.HoursElapsed * 60).ToString("0.00") & " minutes elapsed")
		End If

	End Sub
#End Region
End Class

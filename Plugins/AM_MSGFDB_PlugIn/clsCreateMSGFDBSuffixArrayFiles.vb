'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/29/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsCreateMSGFDBSuffixArrayFiles

	Public Const LEGACY_MSGFDB_SUBDIRECTORY_NAME As String = "Legacy_MSGFDB"
	Protected Const MSGF_PLUS_INDEX_FILE_INFO_SUFFIX As String = ".MSGFPlusIndexFileInfo"

	Protected mErrorMessage As String = String.Empty
	Protected mMgrName As String

	Public ReadOnly Property ErrorMessage As String
		Get
			Return mErrorMessage
		End Get
	End Property

	Public Sub New(strManagerName As String)
		mMgrName = strManagerName
	End Sub

	Protected Function CopyExistingIndexFilesFromRemote(ByVal fiFastaFile As IO.FileInfo, ByVal strMSGFPlusIndexFilesFolderPathBase As String, ByVal blnCheckForLockFile As Boolean, ByVal intDebugLevel As Integer, ByVal sngMaxWaitTimeHours As Single) As IJobParams.CloseOutType

		Dim blnSuccess As Boolean = False

		Try
			Dim strRemoteIndexFolderPath As String
			strRemoteIndexFolderPath = DetermineRemoteMSGFPlusIndexFilesFolderPath(fiFastaFile.Name, strMSGFPlusIndexFilesFolderPathBase)

			Dim diRemoteIndexFolderPath As IO.DirectoryInfo
			diRemoteIndexFolderPath = New IO.DirectoryInfo(strRemoteIndexFolderPath)

			If diRemoteIndexFolderPath.Exists Then

				If blnCheckForLockFile Then
					' Look for an existing lock file
					Dim fiRemoteLockFile As IO.FileInfo
					fiRemoteLockFile = New IO.FileInfo(IO.Path.Combine(diRemoteIndexFolderPath.FullName, fiFastaFile.Name & MSGF_PLUS_INDEX_FILE_INFO_SUFFIX & ".lock"))

					WaitForExistingLockfile(fiRemoteLockFile, intDebugLevel, sngMaxWaitTimeHours)

				End If

				' Look for the .MSGFPlusIndexFileInfo file for this fasta file
				Dim fiMSGFPlusIndexFileInfo As IO.FileInfo
				fiMSGFPlusIndexFileInfo = New IO.FileInfo(IO.Path.Combine(diRemoteIndexFolderPath.FullName, fiFastaFile.Name & MSGF_PLUS_INDEX_FILE_INFO_SUFFIX))

				If fiMSGFPlusIndexFileInfo.Exists Then
					' Read the filenames in the file
					' There should be 3 columns: FileName, FileSize, and FileDateUTC
					' When looking for existing files we only require that the filesize match; FileDateUTC is not used

					Dim dctFilesToCopy As Generic.Dictionary(Of String, Int64)
					dctFilesToCopy = New Generic.Dictionary(Of String, Int64)

					Using srInFile As IO.StreamReader = New IO.StreamReader(New IO.FileStream(fiMSGFPlusIndexFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

						Dim strLineIn As String
						Dim lstData As Generic.List(Of String)

						Do While srInFile.Peek > 0
							strLineIn = srInFile.ReadLine()

							lstData = strLineIn.Split(ControlChars.Tab).ToList()

							If lstData.Count >= 3 Then
								' Add this file to the list of files to copy
								Dim intFileSizeBytes As Int64
								If Int64.TryParse(lstData(1), intFileSizeBytes) Then
									dctFilesToCopy.Add(lstData(0), intFileSizeBytes)
								End If
							End If
						Loop

					End Using

					Dim blnFilesAreValid As Boolean

					If dctFilesToCopy.Count = 0 Then
						blnFilesAreValid = False
					Else
						' Confirm that each file in dctFilesToCopy exists on the remote server
						blnFilesAreValid = ValidateFiles(diRemoteIndexFolderPath.FullName, dctFilesToCopy)
					End If

					If blnFilesAreValid Then
						' Copy each file in lstFilesToCopy (overwrite existing files)
						Dim oFileTools As PRISM.Files.clsFileTools
						Dim strManager As String = GetPseudoManagerName()

						oFileTools = New PRISM.Files.clsFileTools(strManager, intDebugLevel)

						For Each entry As Generic.KeyValuePair(Of String, Int64) In dctFilesToCopy
							Dim fiSourceFile As IO.FileInfo
							Dim strTargetFilePath As String

							fiSourceFile = New IO.FileInfo(IO.Path.Combine(diRemoteIndexFolderPath.FullName, entry.Key))

							strTargetFilePath = IO.Path.Combine(fiFastaFile.Directory.FullName, fiSourceFile.Name)
							oFileTools.CopyFileUsingLocks(fiSourceFile, strTargetFilePath, strManager, True)
						Next

						' Now confirm that each file was successfully copied locally
						blnSuccess = ValidateFiles(fiFastaFile.Directory.FullName, dctFilesToCopy)
					End If

				End If
			End If

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

	Protected Function CopyIndexFilesToRemote(ByVal fiFastaFile As IO.FileInfo, ByVal strMSGFPlusIndexFilesFolderPathBase As String, ByVal intDebugLevel As Integer) As Boolean

		Dim blnSuccess As Boolean = False

		Try
			Dim strRemoteIndexFolderPath As String
			strRemoteIndexFolderPath = DetermineRemoteMSGFPlusIndexFilesFolderPath(fiFastaFile.Name, strMSGFPlusIndexFilesFolderPathBase)

			Dim diRemoteIndexFolderPath As IO.DirectoryInfo
			diRemoteIndexFolderPath = New IO.DirectoryInfo(strRemoteIndexFolderPath)

			If Not diRemoteIndexFolderPath.Parent.Exists Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MSGF+ index files folder not found: " & diRemoteIndexFolderPath.Parent.FullName)
				blnSuccess = False
				Return False
			End If

			If Not diRemoteIndexFolderPath.Exists Then
				diRemoteIndexFolderPath.Create()
			End If

			Dim dctFilesToCopy As Generic.Dictionary(Of String, Int64)
			dctFilesToCopy = New Generic.Dictionary(Of String, Int64)

			Dim lstFileInfo As Generic.List(Of String)
			lstFileInfo = New Generic.List(Of String)

			' Find the index files for fiFastaFile
			For Each fiSourceFile As IO.FileInfo In fiFastaFile.Directory.GetFiles(IO.Path.GetFileNameWithoutExtension(fiFastaFile.Name) & ".*")
				If fiSourceFile.FullName <> fiFastaFile.FullName Then
					If fiSourceFile.Extension <> ".hashcheck" Then
						dctFilesToCopy.Add(fiSourceFile.Name, fiSourceFile.Length)
						lstFileInfo.Add(fiSourceFile.Name & ControlChars.Tab & fiSourceFile.Length & ControlChars.Tab & fiSourceFile.LastWriteTimeUtc.ToString("yyyy-MM-dd hh:mm:ss tt"))
					End If
				End If
			Next

			' Copy up each file
			Dim oFileTools As PRISM.Files.clsFileTools
			Dim strManager As String = GetPseudoManagerName()

			oFileTools = New PRISM.Files.clsFileTools(strManager, intDebugLevel)

			For Each entry As Generic.KeyValuePair(Of String, Int64) In dctFilesToCopy
				Dim strSourceFilePath As String
				Dim strTargetFilePath As String

				strSourceFilePath = IO.Path.Combine(fiFastaFile.Directory.FullName, entry.Key)
				strTargetFilePath = IO.Path.Combine(diRemoteIndexFolderPath.FullName, entry.Key)

				blnSuccess = oFileTools.CopyFileUsingLocks(strSourceFilePath, strTargetFilePath, strManager)
				If Not blnSuccess Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileUsingLocks returned false copying to " & strTargetFilePath)
					Exit For
				End If

			Next

			If blnSuccess Then

				' Create the .MSGFPlusIndexFileInfo file for this fasta file
				Dim fiMSGFPlusIndexFileInfo As IO.FileInfo
				fiMSGFPlusIndexFileInfo = New IO.FileInfo(IO.Path.Combine(diRemoteIndexFolderPath.FullName, fiFastaFile.Name & MSGF_PLUS_INDEX_FILE_INFO_SUFFIX))

				Using swOutFile As IO.StreamWriter = New IO.StreamWriter(New IO.FileStream(fiMSGFPlusIndexFileInfo.FullName, FileMode.Create, FileAccess.Write, FileShare.Read))

					For Each entry As String In lstFileInfo
						swOutFile.WriteLine(entry)
					Next

				End Using

				blnSuccess = True

			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in CopyIndexFilesToRemote; " & ex.Message)
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
	''' <param name="JavaProgLoc"></param>
	''' <param name="MSGFDBProgLoc"></param>
	''' <param name="strFASTAFilePath">Input/output parameter; will get updated if running Legacy MSGFDB</param>
	''' <param name="blnFastaFileIsDecoy">When True, then only creates the forward-based index files.  When False, then creates both the forward and reverse index files</param>
	''' <param name="strMSGFPlusIndexFilesFolderPathBase">Folder path from which to copy (or store) the index files</param>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Function CreateSuffixArrayFiles(
	  ByVal strLogFileDir As String,
	  ByVal intDebugLevel As Integer,
	  ByVal JobNum As String,
	  ByVal JavaProgLoc As String,
	  ByVal MSGFDBProgLoc As String,
	  ByRef strFASTAFilePath As String,
	  ByVal blnFastaFileIsDecoy As Boolean,
	  ByVal strMSGFPlusIndexFilesFolderPathBase As String) As IJobParams.CloseOutType

		Const MAX_WAITTIME_HOURS As Single = 1.0

		Dim strOutputNameBase As String

		Dim fiLockFile As FileInfo
		Dim dbSarrayFilename As String

		Dim sngMaxWaitTimeHours As Single = MAX_WAITTIME_HOURS

		Dim blnMSGFPlus As Boolean
		Dim strCurrentTask As String = "Initializing"
		Dim eResult As IJobParams.CloseOutType = IJobParams.CloseOutType.CLOSEOUT_FAILED

		Try

			mErrorMessage = String.Empty

			If intDebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsCreateMSGFDBSuffixArrayFiles.CreateIndexedDbFiles(): Enter")
			End If

			Dim fiFastaFile As FileInfo
			fiFastaFile = New FileInfo(strFASTAFilePath)

			blnMSGFPlus = IsMSGFPlus(MSGFDBProgLoc)
			If Not blnMSGFPlus Then
				' Running legacy MS-GFDB
				' Create the indexed fasta files in a subdirectory below strFASTAFilePath

				Dim strFASTAFilePathLegacy As String
				If fiFastaFile.Directory.Name.ToLower() = LEGACY_MSGFDB_SUBDIRECTORY_NAME.ToLower() Then
					strFASTAFilePathLegacy = fiFastaFile.FullName
				Else
					strFASTAFilePathLegacy = Path.Combine(Path.Combine(fiFastaFile.DirectoryName, LEGACY_MSGFDB_SUBDIRECTORY_NAME), fiFastaFile.Name)
				End If

				Dim fiFastaFileLegacy As FileInfo
				fiFastaFileLegacy = New FileInfo(strFASTAFilePathLegacy)

				If Not fiFastaFileLegacy.Exists Then
					strCurrentTask = "Creating legacy fasta file folder: " & fiFastaFileLegacy.Directory.FullName
					If Not fiFastaFileLegacy.Directory.Exists Then
						fiFastaFileLegacy.Directory.Create()
					End If
					strCurrentTask = "Copying FASTA file to " & fiFastaFileLegacy.FullName
					fiFastaFile.CopyTo(fiFastaFileLegacy.FullName)
				End If

				' Update strFASTAFilePath and fiFastaFile
				strFASTAFilePath = fiFastaFileLegacy.FullName
				fiFastaFile = New FileInfo(strFASTAFilePath)
			End If

			'  Look for existing suffix array files that 

			strOutputNameBase = Path.GetFileNameWithoutExtension(fiFastaFile.Name)

			fiLockFile = New FileInfo(Path.Combine(fiFastaFile.DirectoryName, strOutputNameBase & "_csarr.lock"))
			dbSarrayFilename = Path.Combine(fiFastaFile.DirectoryName, strOutputNameBase & ".csarr")

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

							If srCannoFile.Peek > -1 Then
								Dim strLine1 As String
								Dim strLine2 As String
								Dim intLine1Value As Integer

								strLine1 = srCannoFile.ReadLine()

								If srCannoFile.Peek > -1 Then
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
			Dim lstFilesToFind As Generic.List(Of String)
			Dim lstExistingFiles As Generic.List(Of String)

			lstFilesToFind = New Generic.List(Of String)
			lstExistingFiles = New Generic.List(Of String)

			If Not blnReindexingRequired Then

				Dim strExistingFiles As String = String.Empty
				Dim strMissingFiles As String = String.Empty

				strCurrentTask = "Validating that expected files exist"
				lstExistingFiles = FindExistingSuffixArrayFiles(blnFastaFileIsDecoy, blnMSGFPlus, strOutputNameBase, fiFastaFile.DirectoryName, lstFilesToFind, strExistingFiles, strMissingFiles)

				If lstExistingFiles.Count < lstFilesToFind.Count Then
					blnReindexingRequired = True

					strCurrentTask = "Some files are missing: " & lstExistingFiles.Count & " vs. " & lstFilesToFind.Count
					If lstExistingFiles.Count > 0 Then
						If intDebugLevel >= 1 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Indexing of " & fiFastaFile.Name & " was incomplete (found " & lstExistingFiles.Count & " out of " & lstFilesToFind.Count & " index files); will repeat the call to BuildSA")
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... existing files: " & strExistingFiles)
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... missing files: " & strMissingFiles)
						End If
					End If
				Else
					blnReindexingRequired = False
				End If

			End If

			If blnReindexingRequired Then

				' Index files are missing or out of date
				' Copy them from strMSGFPlusIndexFilesFolderPathBase if possible
				' Otherwise, create new index files

				Dim blnCheckForLockFile As Boolean

				blnCheckForLockFile = True
				eResult = CopyExistingIndexFilesFromRemote(fiFastaFile, strMSGFPlusIndexFilesFolderPathBase, blnCheckForLockFile, intDebugLevel, sngMaxWaitTimeHours)

				If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
					' Files did not exist, or an error occurred while copying them

					' Create a remote lock file

					Dim fiRemoteLockFile As System.IO.FileInfo = Nothing
					Dim blnRemoteLockFileCreated As Boolean

					strCurrentTask = "Create the remote lock file"
					blnRemoteLockFileCreated = CreateRemoteSuffixArrayLockFile(fiFastaFile.Name, strMSGFPlusIndexFilesFolderPathBase, fiRemoteLockFile, intDebugLevel, sngMaxWaitTimeHours)

					If blnRemoteLockFileCreated Then
						' Lock file successfully created
						' If this manager ended up waiting while another manager was indexing the files, then we should once again try to copy the files locally

						blnCheckForLockFile = False
						eResult = CopyExistingIndexFilesFromRemote(fiFastaFile, strMSGFPlusIndexFilesFolderPathBase, blnCheckForLockFile, intDebugLevel, sngMaxWaitTimeHours)

						If eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
							' Existing files were copied; this manager does not need to re-create them
							blnReindexingRequired = False
						End If
					End If

					If blnReindexingRequired Then
						eResult = CreateSuffixArrayFilesWork(strLogFileDir, intDebugLevel, JobNum, fiFastaFile, fiLockFile, JavaProgLoc, MSGFDBProgLoc, blnFastaFileIsDecoy, blnMSGFPlus, dbSarrayFilename)

						If blnRemoteLockFileCreated AndAlso eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
							CopyIndexFilesToRemote(fiFastaFile, strMSGFPlusIndexFilesFolderPathBase, intDebugLevel)
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
	  ByVal MSGFDBProgLoc As String,
	  ByVal blnFastaFileIsDecoy As Boolean,
	  ByVal blnMSGFPlus As Boolean,
	  ByVal dbSarrayFilename As String) As IJobParams.CloseOutType

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
			If Not File.Exists(MSGFDBProgLoc) Then
				mErrorMessage = "Cannot find " + Path.GetFileName(MSGFDBProgLoc) & " file"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage & ": " & MSGFDBProgLoc)
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If


			' Determine the amount of ram to reserve for BuildSA
			' Examine the size of the .Fasta file to determine how much ram to reserve
			Dim intJavaMemorySizeMB As Integer = 2000

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

			strCurrentTask = "Verify free memory"

			' Make sure the machine has enough free memory to run BuildSA
			If Not clsAnalysisResources.ValidateFreeMemorySize(intJavaMemorySizeMB, "BuildSA", False) Then
				mErrorMessage = "Cannot run BuildSA since less than " & intJavaMemorySizeMB & " MB of free memory"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Create a lock file on the local computer
			If intDebugLevel >= 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating lock file: " & fiLockFile.FullName)
			End If

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

			' Create lock file locally
			Dim bSuccess As Boolean
			strCurrentTask = "Create the local lock file: " & fiLockFile.FullName
			bSuccess = CreateLockFile(fiLockFile.FullName)
			If Not bSuccess Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Delete any existing index files (BuildSA throws an error if they exist)
			strCurrentTask = "Delete any existing files"

			Dim lstExistingFiles As Generic.List(Of String)
			Dim strOutputNameBase As String = Path.GetFileNameWithoutExtension(fiFastaFile.Name)

			lstExistingFiles = FindExistingSuffixArrayFiles(blnFastaFileIsDecoy, blnMSGFPlus, strOutputNameBase, fiFastaFile.DirectoryName, New Generic.List(Of String), String.Empty, String.Empty)

			For Each strFileToDelete In lstExistingFiles
				File.Delete(Path.Combine(fiFastaFile.DirectoryName, strFileToDelete))
			Next

			If intDebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating Suffix Array database file: " & dbSarrayFilename)
			End If

			'Set up and execute a program runner to invoke BuildSA (which is in MSGFDB.jar or MSGFPlus.jar)          
			strCurrentTask = "Construct BuildSA command line"
			Dim CmdStr As String
			CmdStr = " -Xmx" & intJavaMemorySizeMB.ToString & "M -cp " & MSGFDBProgLoc

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
			If Not objBuildSA.RunProgram(JavaProgLoc, CmdStr, "BuildSA", True) Then
				mErrorMessage = "Error running BuildSA in " & Path.GetFileName(MSGFDBProgLoc) & " for " & fiFastaFile.Name
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

	Protected Function CreateRemoteSuffixArrayLockFile(ByVal strFastaFileName As String, ByVal strMSGFPlusIndexFilesFolderPathBase As String, ByRef fiRemoteLockFile As System.IO.FileInfo, ByVal intDebugLevel As Integer, ByVal sngMaxWaitTimeHours As Single) As Boolean

		Dim strCurrentTask As String = "Initializing"

		Dim strRemoteIndexFolderPath As String
		strRemoteIndexFolderPath = DetermineRemoteMSGFPlusIndexFilesFolderPath(strFastaFileName, strMSGFPlusIndexFilesFolderPathBase)

		strCurrentTask = "Looking for folder " & strRemoteIndexFolderPath

		Dim diRemoteIndexFolderPath As IO.DirectoryInfo
		diRemoteIndexFolderPath = New IO.DirectoryInfo(strRemoteIndexFolderPath)

		If Not diRemoteIndexFolderPath.Parent.Exists Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot read/write MSGF+ index files from remote share; folder not found; " & diRemoteIndexFolderPath.FullName)
			Return False
		End If

		fiRemoteLockFile = New IO.FileInfo(IO.Path.Combine(diRemoteIndexFolderPath.FullName, strFastaFileName & MSGF_PLUS_INDEX_FILE_INFO_SUFFIX & ".lock"))

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
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception creating remote MSGF+ suffix array lock file at " & diRemoteIndexFolderPath.FullName & "; " & ex.Message)
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

	Protected Function DetermineRemoteMSGFPlusIndexFilesFolderPath(ByVal strFastaFileName As String, ByVal strMSGFPlusIndexFilesFolderPathBase As String) As String

		Dim reExtractNum As Text.RegularExpressions.Regex = New Text.RegularExpressions.Regex("^ID_(\d+)", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
		Dim reMatch As Text.RegularExpressions.Match

		Dim strRemoteIndexFolderPath As String = "Other"

		' DMS-generated fasta files will have a name of the form ID_003949_3D6802EE.fasta
		' Parse out the number (003949 in this case)
		reMatch = reExtractNum.Match(strFastaFileName)
		If reMatch.Success Then
			Dim intGeneratedFastaFileNumber As Integer

			If Integer.TryParse(reMatch.Groups.Item(1).Value, intGeneratedFastaFileNumber) Then
				' Round down to the nearest 1000
				' Thus, 003949 will round to 3000
				strRemoteIndexFolderPath = (Math.Floor(intGeneratedFastaFileNumber / 1000.0) * 1000).ToString("0")
			End If
		End If

		strRemoteIndexFolderPath = Path.Combine(strMSGFPlusIndexFilesFolderPathBase, strRemoteIndexFolderPath)

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
	''' <param name="strExistingFiles">Output param: semicolon separated list of existing files</param>
	''' <param name="strMissingFiles">Output param: semicolon separated list of missing files</param>
	''' <returns>A list of the files that currently exist</returns>
	''' <remarks></remarks>
	Protected Function FindExistingSuffixArrayFiles(
	  ByVal blnFastaFileIsDecoy As Boolean,
	  ByVal blnMSGFPlus As Boolean,
	  ByVal strOutputNameBase As String,
	  ByVal strFolderPathToSearch As String,
	  ByRef lstFilesToFind As Generic.List(Of String),
	  ByRef strExistingFiles As String,
	  ByRef strMissingFiles As String) As Generic.List(Of String)

		Dim lstExistingFiles As Generic.List(Of String)
		lstExistingFiles = New Generic.List(Of String)

		If lstFilesToFind Is Nothing Then
			lstFilesToFind = New Generic.List(Of String)
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

			If File.Exists(Path.Combine(strFolderPathToSearch, strFileNameToFind)) Then
				lstExistingFiles.Add(strFileNameToFind)
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

		Dim fiJarFile As FileInfo
		fiJarFile = New FileInfo(MSGFDBJarFilePath)

		If fiJarFile.Name.ToLower() = AnalysisManagerMSGFDBPlugIn.clsMSGFDBUtils.MSGFDB_JAR_NAME.ToLower() Then
			Return False
		Else
			Return True
		End If

	End Function

	''' <summary>
	''' Verifies that each of the files specified by dctFilesToCopy exists at strFolderPathToCheck and has the correct file size
	''' </summary>
	''' <param name="strFolderPathToCheck">folder to check</param>
	''' <param name="dctFilesToCopy">Dictionary with filenames and file sizes</param>
	''' <returns>True if all files are found and are the right size</returns>
	''' <remarks></remarks>
	Protected Function ValidateFiles(ByVal strFolderPathToCheck As String, ByVal dctFilesToCopy As Generic.Dictionary(Of String, Int64)) As Boolean

		For Each entry As Generic.KeyValuePair(Of String, Int64) In dctFilesToCopy
			Dim fiSourceFile As IO.FileInfo
			fiSourceFile = New IO.FileInfo(IO.Path.Combine(strFolderPathToCheck, entry.Key))

			If Not fiSourceFile.Exists Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Remote MSGF+ index file not found: " & fiSourceFile.FullName)
				Return False
			ElseIf fiSourceFile.Length <> entry.Value Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Remote MSGF+ index file is not the expected size: " & fiSourceFile.FullName & " should be " & entry.Value & " bytes but is actually " & fiSourceFile.Length & " bytes")
				Return False
			End If
		Next

		Return True

	End Function

	Protected Sub WaitForExistingLockfile(ByVal fiLockFile As IO.FileInfo, ByVal intDebugLevel As Integer, ByVal sngMaxWaitTimeHours As Single)

		' Check to see if another Analysis Manager is already creating the indexed DB files
		If fiLockFile.Exists AndAlso System.DateTime.UtcNow.Subtract(fiLockFile.LastWriteTimeUtc).TotalMinutes >= 60 Then
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
				System.Threading.Thread.Sleep(2000)

				If System.DateTime.UtcNow.Subtract(fiLockFile.CreationTimeUtc).TotalHours >= sngMaxWaitTimeHours Then
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

End Class

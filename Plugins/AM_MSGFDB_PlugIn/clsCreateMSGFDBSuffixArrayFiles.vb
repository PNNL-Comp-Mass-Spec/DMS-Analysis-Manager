'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/29/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsCreateMSGFDBSuffixArrayFiles

	Public Const LEGACY_MSGFDB_SUBDIRECTORY_NAME As String = "Legacy_MSGFDB"

	Protected mErrorMessage As String = String.Empty

	Public ReadOnly Property ErrorMessage As String
		Get
			Return mErrorMessage
		End Get
	End Property

	''' <summary>
	''' Convert .Fasta file to indexed DB files compatible with MSGFPlus
	''' </summary>
	''' <param name="strLogfileDir"></param>
	''' <param name="intDebugLevel"></param>
	''' <param name="JobNum"></param>
	''' <param name="JavaProgLoc"></param>
	''' <param name="MSGFDBProgLoc"></param>
	''' <param name="strFASTAFilePath">Input/output parameter; will get updated if running Legacy MSGFDB</param>
	''' <param name="blnFastaFileIsDecoy">
	''' </param>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Function CreateSuffixArrayFiles(ByVal strLogFileDir As String, _
	  ByVal intDebugLevel As Integer, _
	  ByVal JobNum As String, _
	  ByVal JavaProgLoc As String,
	  ByVal MSGFDBProgLoc As String, _
	  ByRef strFASTAFilePath As String, _
	  ByVal blnFastaFileIsDecoy As Boolean) As IJobParams.CloseOutType

		Const MAX_WAITTIME_HOURS As Single = 1.0

		Dim strOutputNameBase As String

		Dim dbLockFilename As String
		Dim dbSarrayFilename As String

		Dim fi As System.IO.FileInfo
		Dim createTime As DateTime
		Dim durationTime As TimeSpan
		Dim currentTime As DateTime
		Dim sngMaxWaitTimeHours As Single = MAX_WAITTIME_HOURS

		Dim blnMSGFPlus As Boolean
		Dim strCurrentTask As String = "Initializing"

		Try

			mErrorMessage = String.Empty

			If intDebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsCreateMSGFDBSuffixArrayFiles.CreateIndexedDbFiles(): Enter")
			End If

			Dim fiFastaFile As System.IO.FileInfo
			fiFastaFile = New System.IO.FileInfo(strFASTAFilePath)

			blnMSGFPlus = IsMSGFPlus(MSGFDBProgLoc)
			If Not blnMSGFPlus Then
				' Running legacy MS-GFDB
				' Create the indexed fasta files in a subdirectory below strFASTAFilePath

				Dim strFASTAFilePathLegacy As String
				If fiFastaFile.Directory.Name.ToLower() = LEGACY_MSGFDB_SUBDIRECTORY_NAME.ToLower() Then
					strFASTAFilePathLegacy = fiFastaFile.FullName
				Else
					strFASTAFilePathLegacy = IO.Path.Combine(IO.Path.Combine(fiFastaFile.DirectoryName, LEGACY_MSGFDB_SUBDIRECTORY_NAME), fiFastaFile.Name)
				End If

				Dim fiFastaFileLegacy As System.IO.FileInfo
				fiFastaFileLegacy = New IO.FileInfo(strFASTAFilePathLegacy)

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
				fiFastaFile = New IO.FileInfo(strFASTAFilePath)
			End If

			strOutputNameBase = System.IO.Path.GetFileNameWithoutExtension(fiFastaFile.Name)

			dbLockFilename = System.IO.Path.Combine(fiFastaFile.DirectoryName, strOutputNameBase & "_csarr.lock")
			dbSarrayFilename = System.IO.Path.Combine(fiFastaFile.DirectoryName, strOutputNameBase & ".csarr")

			' Check to see if another Analysis Manager is already creating the indexed DB files
			strCurrentTask = "Looking for lock file " & dbLockFilename
			If System.IO.File.Exists(dbLockFilename) Then
				If intDebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Lock file found: " & dbLockFilename & "; waiting for file to be removed by other manager generating .csarr file " & System.IO.Path.GetFileName(dbSarrayFilename))
				End If

				' Lock file found; wait up to sngMaxWaitTimeHours hours
				fi = My.Computer.FileSystem.GetFileInfo(dbLockFilename)
				createTime = fi.CreationTimeUtc
				currentTime = System.DateTime.UtcNow
				durationTime = currentTime - createTime
				While System.IO.File.Exists(dbLockFilename) And durationTime.Hours < sngMaxWaitTimeHours
					' Sleep for 2 seconds
					System.Threading.Thread.Sleep(2000)

					' Update the current time and elapsed duration
					currentTime = System.DateTime.UtcNow
					durationTime = currentTime - createTime
				End While

				'If the duration time has exceeded sngMaxWaitTimeHours, then delete the lock file and try again with this manager
				If durationTime.Hours > sngMaxWaitTimeHours Then
					strCurrentTask = "Waited over " & sngMaxWaitTimeHours.ToString("0.0") & " hour(s) for lock file: " & dbLockFilename & " to be deleted, but it is still present; deleting the file now and continuing"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strCurrentTask)
					If System.IO.File.Exists(dbLockFilename) Then
						System.IO.File.Delete(dbLockFilename)
					End If
				End If

			End If

			' Validate that all of the expected files exist
			' If any are missing, then need to repeat the call to "BuildSA"
			Dim blnReindexingRequired As Boolean = False

			strCurrentTask = "Validating that expected files exist"
			If blnMSGFPlus Then
				' Check for any FastaFileName.revConcat.* files
				' If they exist, delete them, since they are for legacy MSGFDB

				Dim fiLegacyIndexedFiles() As IO.FileInfo
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
					Dim fiCAnnoFile As System.IO.FileInfo
					fiCAnnoFile = New IO.FileInfo(IO.Path.Combine(fiFastaFile.DirectoryName, strOutputNameBase & ".canno"))
					If fiCAnnoFile.Exists Then

						strCurrentTask = "Examining first two lines of " & fiCAnnoFile.FullName
						Using srCannoFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(fiCAnnoFile.FullName, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

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
			Dim lstFilesToFind As System.Collections.Generic.List(Of String)
			Dim lstExistingFiles As System.Collections.Generic.List(Of String)

			lstFilesToFind = New System.Collections.Generic.List(Of String)
			lstExistingFiles = New System.Collections.Generic.List(Of String)

			If Not blnReindexingRequired Then

				' Old suffixes (used prior to August 2011)
				'lstFilesToFind.Add(".revConcat.fasta")
				'lstFilesToFind.Add(".seq")
				'lstFilesToFind.Add(".seqanno")
				'lstFilesToFind.Add(".revConcat.seq")
				'lstFilesToFind.Add(".revConcat.seqanno")
				'lstFilesToFind.Add(".sarray")
				'lstFilesToFind.Add(".revConcat.sarray")

				' Suffixes for MSGFDB (effective 8/22/2011) and and MSGF+ 
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

				Dim strExistingFiles As String = String.Empty
				Dim strMissingFiles As String = String.Empty

				strCurrentTask = "Validating that expected files exist"
				For Each strSuffix In lstFilesToFind

					Dim strFileNameToFind As String = strOutputNameBase & strSuffix

					If System.IO.File.Exists(System.IO.Path.Combine(fiFastaFile.DirectoryName, strFileNameToFind)) Then
						lstExistingFiles.Add(strFileNameToFind)
						strExistingFiles = clsGlobal.AppendToComment(strExistingFiles, strFileNameToFind)
					Else
						strMissingFiles = clsGlobal.AppendToComment(strMissingFiles, strFileNameToFind)
					End If
				Next

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

			' If lock file existed, the SuffixArray files should now be created
			' Check for one of the index files in case this is the first time or in case
			' there was a problem with another manager creating it.
			If blnReindexingRequired Then
				' Try to create the index files for fasta file strDBFileNameInput
				strCurrentTask = "Look for java.exe and .jar file"

				' Verify that Java exists
				If Not System.IO.File.Exists(JavaProgLoc) Then
					mErrorMessage = "Cannot find Java program file"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage & ": " & JavaProgLoc)
					Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
				End If

				' Verify that the MSGFDB.Jar or MSGFPlus.jar file exists
				If Not System.IO.File.Exists(MSGFDBProgLoc) Then
					mErrorMessage = "Cannot find " + IO.Path.GetFileName(MSGFDBProgLoc) & " file"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage & ": " & MSGFDBProgLoc)
					Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
				End If


				' Determine the amount of ram to reserve for BuildSA
				' Examine the size of the .Fasta file to determine how much ram to reserve
				Dim intJavaMemorySizeMB As Integer = 2000

				Dim intFastaFileSizeMB As Integer
				intFastaFileSizeMB = CInt(fiFastaFile.Length / 1024.0 / 1024.0)

				If intFastaFileSizeMB <= 125 Then
					intJavaMemorySizeMB = 2000
				ElseIf intFastaFileSizeMB <= 250 Then
					intJavaMemorySizeMB = 4000
				ElseIf intFastaFileSizeMB <= 375 Then
					intJavaMemorySizeMB = 6000
				Else
					intJavaMemorySizeMB = 8000
				End If

				strCurrentTask = "Verify free memory"

				' Make sure the machine has enough free memory to run BuildSA
				If Not clsAnalysisResources.ValidateFreeMemorySize(intJavaMemorySizeMB, "BuildSA", False) Then
					mErrorMessage = "Cannot run BuildSA since less than " & intJavaMemorySizeMB & " MB of free memory"
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				If intDebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating lock file: " & dbLockFilename)
				End If

				' Check one more time for a lock file
				' If it exists, then another manager just created it and we should bort
				strCurrentTask = "Look for the lock file one last time"
				If System.IO.File.Exists(dbLockFilename) Then
					If intDebugLevel >= 1 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Warning: new lock file found: " & dbLockFilename & "; aborting")
						Return IJobParams.CloseOutType.CLOSEOUT_NO_FAS_FILES
					End If
				End If

				' Create lock file
				Dim bSuccess As Boolean
				strCurrentTask = "Create the lock file: " & dbLockFilename
				bSuccess = CreateLockFile(dbLockFilename)
				If Not bSuccess Then
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				' Delete any existing index files (BuildSA throws an error if they exist)
				strCurrentTask = "Delete any existing files"
				For Each strFileToDelete In lstExistingFiles
					System.IO.File.Delete(System.IO.Path.Combine(fiFastaFile.DirectoryName, strFileToDelete))
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
					.ConsoleOutputFilePath = System.IO.Path.Combine(strLogFileDir, "MSGFDB_BuildSA_ConsoleOutput.txt")
				End With

				strCurrentTask = "Run BuildSA using " & CmdStr
				If Not objBuildSA.RunProgram(JavaProgLoc, CmdStr, "BuildSA", True) Then
					mErrorMessage = "Error running BuildSA in " & IO.Path.GetFileName(MSGFDBProgLoc) & " for " & fiFastaFile.Name
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mErrorMessage & ": " & JobNum)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				Else
					If intDebugLevel >= 1 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Created suffix array files for " & fiFastaFile.Name)
					End If
				End If

				If intDebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting lock file: " & dbLockFilename)
				End If

				' Delete the lock file
				strCurrentTask = "Delete the lock file"
				If System.IO.File.Exists(dbLockFilename) Then
					System.IO.File.Delete(dbLockFilename)
				End If
			End If

		Catch ex As Exception
			mErrorMessage = "Exception in .CreateIndexedDbFiles"
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
			Using sw As System.IO.StreamWriter = New System.IO.StreamWriter(strLockFilePath)
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

	Public Function IsMSGFPlus(ByVal MSGFDBJarFilePath As String) As Boolean

		Dim fiJarFile As System.IO.FileInfo
		fiJarFile = New System.IO.FileInfo(MSGFDBJarFilePath)

		If fiJarFile.Name.ToLower() = AnalysisManagerMSGFDBPlugIn.clsMSGFDBUtils.MSGFDB_JAR_NAME.ToLower() Then
			Return False
		Else
			Return True
		End If

	End Function
End Class

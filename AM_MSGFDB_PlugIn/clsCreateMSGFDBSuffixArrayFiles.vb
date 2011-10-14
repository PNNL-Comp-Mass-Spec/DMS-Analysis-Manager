'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/29/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsCreateMSGFDBSuffixArrayFiles

	Protected mErrorMessage As String = String.Empty

	Public ReadOnly Property ErrorMessage As String
		Get
			Return mErrorMessage
		End Get
	End Property

	''' <summary>
	''' Convert .Fasta file to indexed DB files compatible with MSGFDB
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Function CreateSuffixArrayFiles(ByVal strLogfileDir As String, _
										   ByVal intDebugLevel As Integer, _
										   ByVal JobNum As String, _
										   ByVal JavaProgLoc As String,
										   ByVal MSGFDBProgLoc As String, _
										   ByVal strFASTAFilePath As String, _
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

		Try

			mErrorMessage = String.Empty

			If intDebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsCreateMSGFDBSuffixArrayFiles.CreateIndexedDbFiles(): Enter")
			End If

			Dim fiFastaFile As System.IO.FileInfo
			fiFastaFile = New System.IO.FileInfo(strFASTAFilePath)

			strOutputNameBase = System.IO.Path.GetFileNameWithoutExtension(fiFastaFile.Name)

			dbLockFilename = System.IO.Path.Combine(fiFastaFile.DirectoryName, strOutputNameBase & "_csarr.lock")
			dbSarrayFilename = System.IO.Path.Combine(fiFastaFile.DirectoryName, strOutputNameBase & ".csarr")

			' Check to see if another Analysis Manager is already creating the indexed DB files
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
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Waited over " & sngMaxWaitTimeHours.ToString("0.0") & " hour(s) for lock file: " & dbLockFilename & " to be deleted, but it is still present; deleting the file now and continuing")
					If System.IO.File.Exists(dbLockFilename) Then
						System.IO.File.Delete(dbLockFilename)
					End If
				End If

			End If

			' Validate that all of the expected files exist
			' If any are missing, then need to repeat the call to "BuildSA"
			Dim blnFilesMissing As Boolean

			' This dictionary contains file suffixes to look for
			' Keys will be "True" if the file exists and false if it does not exist
			Dim lstFilesToFind As System.Collections.Generic.List(Of String)
			Dim lstExistingFiles As System.Collections.Generic.List(Of String)

			lstFilesToFind = New System.Collections.Generic.List(Of String)

			' Old suffixes (used prior to August 2011)
			'lstFilesToFind.Add(".revConcat.fasta")
			'lstFilesToFind.Add(".seq")
			'lstFilesToFind.Add(".seqanno")
			'lstFilesToFind.Add(".revConcat.seq")
			'lstFilesToFind.Add(".revConcat.seqanno")
			'lstFilesToFind.Add(".sarray")
			'lstFilesToFind.Add(".revConcat.sarray")

			' New suffixes (effective 8/22/2011)
			lstFilesToFind.Add(".canno")
			lstFilesToFind.Add(".cnlcp")
			lstFilesToFind.Add(".csarr")
			lstFilesToFind.Add(".cseq")

			If Not blnFastaFileIsDecoy Then
				lstFilesToFind.Add(".revConcat.canno")
				lstFilesToFind.Add(".revConcat.cnlcp")
				lstFilesToFind.Add(".revConcat.csarr")
				lstFilesToFind.Add(".revConcat.cseq")
				lstFilesToFind.Add(".revConcat.fasta")
			End If

			Dim strExistingFiles As String = String.Empty
			Dim strMissingFiles As String = String.Empty

			lstExistingFiles = New System.Collections.Generic.List(Of String)
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
				blnFilesMissing = True

				If lstExistingFiles.Count > 0 Then
					If intDebugLevel >= 1 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Indexing of " & fiFastaFile.Name & " was incomplete (found " & lstExistingFiles.Count & " out of " & lstFilesToFind.Count & " index files); will repeat the call to BuildSA")
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... existing files: " & strExistingFiles)
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... missing files: " & strMissingFiles)
					End If
				End If
			Else
				blnFilesMissing = False
			End If

			' If lock file existed, the SuffixArray files should now be created
			' Check for one of the index files in case this is the first time or in case
			' there was a problem with another manager creating it.
			If blnFilesMissing Then
				' Try to create the index files for fasta file strDBFileNameInput

				' Verify that Java exists
				If Not System.IO.File.Exists(JavaProgLoc) Then
					mErrorMessage = "Cannot find Java program file"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage & ": " & JavaProgLoc)
					Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
				End If

				' Verify that the MSGFDB.Jar file exists
				If Not System.IO.File.Exists(MSGFDBProgLoc) Then
					mErrorMessage = "Cannot find MSGFDB.Jar file"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage & ": " & MSGFDBProgLoc)
					Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
				End If


				' Reserve 2 GB of ram for BuildSA; Sangtae says this should accomodate at least a 200 MB fasta file
				Dim intJavaMemorySizeMB As Integer = 2000

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
				If System.IO.File.Exists(dbLockFilename) Then
					If intDebugLevel >= 1 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Warning: new lock file found: " & dbLockFilename & "; aborting")
						Return IJobParams.CloseOutType.CLOSEOUT_NO_FAS_FILES
					End If
				End If

				' Create lock file
				Dim bSuccess As Boolean
				bSuccess = CreateLockFile(dbLockFilename)
				If Not bSuccess Then
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				' Delete any existing index files (BuildSA throws an error if they exist)
				For Each strFileToDelete In lstExistingFiles
					System.IO.File.Delete(System.IO.Path.Combine(fiFastaFile.DirectoryName, strFileToDelete))
				Next

				If intDebugLevel >= 2 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating Suffix Array database file: " & dbSarrayFilename)
				End If

				'Set up and execute a program runner to invoke BuildSA (which is in MSGFDB.jar)          

				Dim CmdStr As String
				CmdStr = " -Xmx" & intJavaMemorySizeMB.ToString & "M -cp " & MSGFDBProgLoc & " msdbsearch.BuildSA -d " & fiFastaFile.FullName

				If blnFastaFileIsDecoy Then
					CmdStr &= " -tda 0"
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
					.ConsoleOutputFilePath = System.IO.Path.Combine(strLogfileDir, "MSGFDB_BuildSA_ConsoleOutput.txt")
				End With

				If Not objBuildSA.RunProgram(JavaProgLoc, CmdStr, "BuildSA", True) Then
					mErrorMessage = "Error running BuildSA in MSGFDB.Jar for " & fiFastaFile.Name
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
				If System.IO.File.Exists(dbLockFilename) Then
					System.IO.File.Delete(dbLockFilename)
				End If
			End If

		Catch ex As Exception
			mErrorMessage = "Exception in .CreateIndexedDbFiles"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage & ": " & ex.Message)
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
			Dim sw As System.IO.StreamWriter = New System.IO.StreamWriter(strLockFilePath)

			' Add Date and time to the file.
			sw.WriteLine(DateTime.Now)
			sw.Close()
			sw.Dispose()

		Catch ex As Exception
			mErrorMessage = "Error creating lock file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsCreateMSGFDBSuffixArrayFiles.CreateLockFile, " & mErrorMessage & ": " & ex.Message)
			Return False
		End Try

		Return True

	End Function

End Class

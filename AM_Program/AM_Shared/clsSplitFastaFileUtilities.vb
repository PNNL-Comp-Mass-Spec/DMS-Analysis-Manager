Imports System.IO
Imports System.Threading
Imports System.Runtime.InteropServices
Imports FastaFileSplitterDLL

Public Class clsSplitFastaFileUtilities

	Public Const LOCK_FILE_PROGRESS_TEXT As String = "Lockfile"

	Protected Const SP_NAME_UPDATE_ORGANISM_DB_FILE As String = "AddUpdateOrganismDBFile"
	Protected Const SP_NAME_REFRESH_CACHED_ORG_DB_INFO As String = "RefreshCachedOrganismDBInfo"

	Protected mDMSConnectionString As String
	Protected mProteinSeqsDBConnectionString As String
	Protected mMSGFPlusIndexFilesFolderPathLegacyDB As String

	ReadOnly mNumSplitParts As Integer

	Protected mErrorMessage As String

	Protected mWaitingForLockFile As Boolean

	Protected WithEvents mSplitter As clsFastaFileSplitter

	Public ReadOnly Property ErrorMessage As String
		Get
			Return mErrorMessage
		End Get
	End Property

	Public Property MSGFPlusIndexFilesFolderPathLegacyDB() As String
		Get
			Return mMSGFPlusIndexFilesFolderPathLegacyDB
		End Get
		Set(value As String)
			mMSGFPlusIndexFilesFolderPathLegacyDB = value
		End Set
	End Property

	Public ReadOnly Property WaitingForLockFile() As Boolean
		Get
			Return mWaitingForLockFile
		End Get
	End Property

	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="dmsConnectionString"></param>
	''' <param name="proteinSeqsDBConnectionString"></param>
	''' <param name="numSplitParts"></param>
	''' <remarks></remarks>
	Public Sub New(ByVal dmsConnectionString As String, ByVal proteinSeqsDBConnectionString As String, ByVal numSplitParts As Integer)

		mDMSConnectionString = dmsConnectionString
		mProteinSeqsDBConnectionString = proteinSeqsDBConnectionString
		mNumSplitParts = numSplitParts

		mMSGFPlusIndexFilesFolderPathLegacyDB = "\\Proto-7\MSGFPlus_Index_Files\Other"

		mErrorMessage = String.Empty
		mWaitingForLockFile = False

	End Sub

	''' <summary>
	''' Creates a new lock file to allow the calling process to either create the split fasta file or validate that the split fasta file exists
	''' </summary>
	''' <param name="fiBaseFastaFile"></param>
	''' <param name="lockFilePath">Output parameter: path to the newly created lock file</param>
	''' <returns>Lock file handle</returns>
	''' <remarks></remarks>
	Protected Function CreateLockStream(ByVal fiBaseFastaFile As FileInfo, <Out()> ByRef lockFilePath As String) As FileStream

		Dim startTime As DateTime = DateTime.UtcNow
		Dim intAttemptCount As Integer = 0

		Dim lockStream As FileStream

		lockFilePath = Path.Combine(fiBaseFastaFile.FullName + ".lock")
		Dim lockFi = New FileInfo(lockFilePath)

		Do
			intAttemptCount += 1

			Try
				lockFi.Refresh()
				If lockFi.Exists Then
					mWaitingForLockFile = True

					Dim LockTimeoutTime As DateTime = lockFi.LastWriteTimeUtc.AddMinutes(60)
					OnProgressUpdate(LOCK_FILE_PROGRESS_TEXT & " found; waiting until it is deleted or until " & LockTimeoutTime.ToLocalTime().ToString() & ": " & lockFi.Name, 0)

					While lockFi.Exists AndAlso DateTime.UtcNow < LockTimeoutTime
						Thread.Sleep(5000)
						lockFi.Refresh()
						If DateTime.UtcNow.Subtract(startTime).TotalMinutes >= 60 Then
							Exit While
						End If
					End While

					lockFi.Refresh()
					If lockFi.Exists Then
						OnProgressUpdate(LOCK_FILE_PROGRESS_TEXT & " still exists; assuming another process timed out; thus, now deleting file " & lockFi.Name, 0)
						lockFi.Delete()
					End If

					mWaitingForLockFile = False

				End If

				' Try to create a lock file so that the calling procedure can create the required .Fasta file (or validate that it now exists)

				' Try to create the lock file
				' If another process is still using it, an exception will be thrown
				lockStream = lockFi.Create()

				' We have successfully created a lock file, 
				' so we should exit the Do Loop
				Exit Do

			Catch ex As Exception
				OnProgressUpdate("Exception while monitoring " & LOCK_FILE_PROGRESS_TEXT & " " & lockFi.FullName & ": " & ex.Message, 0)
			End Try

			' Something went wrong; wait for 15 seconds then try again
			Thread.Sleep(15000)

			If intAttemptCount >= 4 Then
				' Something went wrong 4 times in a row (typically either creating or deleting the .Lock file)
				' Abort

				' Exception: Unable to create Lockfile required to split fasta file ...
				Throw New Exception("Unable to create " & LOCK_FILE_PROGRESS_TEXT & " required to split fasta file " & fiBaseFastaFile.FullName & "; tried 4 times without success")
			End If
		Loop

		Return lockStream

	End Function

	Protected Sub DeleteLockStream(ByVal lockFilePath As String, ByVal lockStream As IO.FileStream)

		Try
			If Not lockStream Is Nothing Then
				lockStream.Close()
			End If

			Dim retryCount As Integer = 3

			While retryCount > 0
				Try

					Dim lockFi = New FileInfo(lockFilePath)
					If lockFi.Exists Then
						lockFi.Delete()
					End If
					Exit While

				Catch ex As Exception
					OnErrorEvent("Exception deleting lock file in DeleteLockStream: " & ex.Message)
					retryCount -= 1
					Dim oRandom = New Random()
					Thread.Sleep(oRandom.Next(100, 500))
				End Try
			End While

		Catch ex As Exception
			OnErrorEvent("Exception in DeleteLockStream: " & ex.Message)
		End Try

	End Sub

	''' <summary>
	''' Lookup the details for LegacyFASTAFileName in the database
	''' </summary>
	''' <param name="legacyFASTAFileName"></param>
	''' <param name="organismName">Output parameter: the organism name for this fasta file</param>
	''' <returns>The path to the file if found; empty string if no match</returns>
	''' <remarks></remarks>
	Protected Function GetLegacyFastaFilePath(ByVal legacyFASTAFileName As String, <Out()> ByRef organismName As String) As String

		Const retryCount As Short = 3

		Dim SqlStr As Text.StringBuilder = New Text.StringBuilder

		organismName = String.Empty

		' Query V_Legacy_Static_File_Locations for the path to the fasta file
		'
		SqlStr.Append(" SELECT TOP 1 Full_Path, Organism_Name ")
		SqlStr.Append(" FROM V_Legacy_Static_File_Locations")
		SqlStr.Append(" WHERE FileName = '" & legacyFASTAFileName & "'")

		Dim dtResults As DataTable = Nothing

		Dim blnSuccess = clsGlobal.GetDataTableByQuery(SqlStr.ToString(), mProteinSeqsDBConnectionString, "GetLegacyFastaFilePath", retryCount, dtResults)

		If Not blnSuccess Then
			Return String.Empty
		End If

		For Each CurRow As DataRow In dtResults.Rows

			Dim legacyFASTAFilePath = clsGlobal.DbCStr(CurRow(0))
			organismName = clsGlobal.DbCStr(CurRow(1))

			Return legacyFASTAFilePath
		Next

		' Database query was successful, but no rows were returned
		Return String.Empty

	End Function

	Protected Function StoreSplitFastaFileNames(ByVal organismName As String, ByVal lstSplitFastaInfo As List(Of clsFastaFileSplitter.udtFastaFileInfoType)) As Boolean

		Dim splitFastaName As String = "??"

		Try

			For Each udtFileInfo In lstSplitFastaInfo
				' Add/update each split file

				Dim fiSplitFastaFile = New FileInfo(udtFileInfo.FilePath)
				splitFastaName = fiSplitFastaFile.Name

				'Setup for execution of the stored procedure
				Dim MyCmd As New SqlClient.SqlCommand
				With MyCmd
					.CommandType = CommandType.StoredProcedure
					.CommandText = SP_NAME_UPDATE_ORGANISM_DB_FILE

					.Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int))
					.Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue

					.Parameters.Add(New SqlClient.SqlParameter("@FastaFileName", SqlDbType.VarChar, 128))
					.Parameters.Item("@FastaFileName").Direction = ParameterDirection.Input
					.Parameters.Item("@FastaFileName").Value = splitFastaName

					.Parameters.Add(New SqlClient.SqlParameter("@OrganismName", SqlDbType.VarChar, 128))
					.Parameters.Item("@OrganismName").Direction = ParameterDirection.Input
					.Parameters.Item("@OrganismName").Value = organismName

					.Parameters.Add(New SqlClient.SqlParameter("@NumProteins", SqlDbType.Int))
					.Parameters.Item("@NumProteins").Direction = ParameterDirection.Input
					.Parameters.Item("@NumProteins").Value = udtFileInfo.NumProteins

					.Parameters.Add(New SqlClient.SqlParameter("@NumResidues", SqlDbType.BigInt))
					.Parameters.Item("@NumResidues").Direction = ParameterDirection.Input
					.Parameters.Item("@NumResidues").Value = udtFileInfo.NumResidues

                    .Parameters.Add(New SqlClient.SqlParameter("@FileSizeKB", SqlDbType.Int))
                    .Parameters.Item("@FileSizeKB").Direction = ParameterDirection.Input
                    .Parameters.Item("@FileSizeKB").Value = (fiSplitFastaFile.Length / 1024.0).ToString("0")

                    .Parameters.Add(New SqlClient.SqlParameter("@Message", SqlDbType.VarChar, 512))
					.Parameters.Item("@Message").Direction = ParameterDirection.InputOutput
					.Parameters.Item("@Message").Value = String.Empty

				End With


				Dim retryCount = 3
				While retryCount > 0
					Try
						Using connection As SqlClient.SqlConnection = New SqlClient.SqlConnection(mDMSConnectionString)
							connection.Open()
							MyCmd.Connection = connection
							MyCmd.ExecuteNonQuery()

							Dim resultCode = CInt(MyCmd.Parameters("@Return").Value)

							If resultCode <> 0 Then
								' Error occurred
								mErrorMessage = SP_NAME_UPDATE_ORGANISM_DB_FILE & " returned a non-zero error code of " & resultCode

								Dim statusMessage = MyCmd.Parameters("@Message").Value
								If Not statusMessage Is Nothing Then
									mErrorMessage = mErrorMessage & "; " & CStr(statusMessage)
								End If

								OnErrorEvent(mErrorMessage)
								Return False
							End If

						End Using

						Exit While

					Catch ex As Exception
						retryCount -= 1S
						mErrorMessage = "Exception storing fasta file " & splitFastaName & " in T_Organism_DB_File: " + ex.Message
						OnErrorEvent(mErrorMessage)
						' Delay for 2 seconds before trying again
						Thread.Sleep(2000)

					End Try

				End While

			Next

		Catch ex As Exception
			mErrorMessage = "Exception in StoreSplitFastaFileNames for " + splitFastaName & ": " & ex.Message
			OnErrorEvent(mErrorMessage)
			Return False
		End Try

		Return True

	End Function

	Protected Sub UpdateCachedOrganismDBInfo()

		Try

			'Setup for execution of the stored procedure
			Dim MyCmd As New SqlClient.SqlCommand
			With MyCmd
				.CommandType = CommandType.StoredProcedure
				.CommandText = SP_NAME_REFRESH_CACHED_ORG_DB_INFO

				.Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int))
				.Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue
			End With


			Dim retryCount = 3
			While retryCount > 0
				Try
					Using connection As SqlClient.SqlConnection = New SqlClient.SqlConnection(mProteinSeqsDBConnectionString)
						connection.Open()
						MyCmd.Connection = connection
						MyCmd.ExecuteNonQuery()

						Dim resultCode = CInt(MyCmd.Parameters("@Return").Value)

						If resultCode <> 0 Then
							' Error occurred
							OnErrorEvent("Call to " & SP_NAME_REFRESH_CACHED_ORG_DB_INFO & " returned a non-zero error code: " & resultCode)
						End If

					End Using

					Exit While

				Catch ex As Exception
					retryCount -= 1S
					mErrorMessage = "Exception updating the cached organism DB info on ProteinSeqs: " + ex.Message
					OnErrorEvent(mErrorMessage)
					' Delay for 2 seconds before trying again
					Thread.Sleep(2000)

				End Try

			End While

		Catch ex As Exception
			mErrorMessage = "Exception in UpdateCachedOrganismDBInfo: " & ex.Message
			OnErrorEvent(mErrorMessage)
		End Try

	End Sub


	''' <summary>
	''' Validate that the split fasta file exists
	''' </summary>
	''' <param name="baseFastaName">Original (non-split) filename, e.g. RefSoil_2013-11-07.fasta</param>
	''' <param name="splitFastaName">Split fasta filename, e.g. RefSoil_2013-11-07_10x_05.fasta</param>
	''' <returns>True if the split fasta file is defined in DMS</returns>
	''' <remarks>If the split file is not found, then will automatically split the original file and update DMS with the split file information</remarks>
	Public Function ValidateSplitFastaFile(ByVal baseFastaName As String, ByVal splitFastaName As String) As Boolean

		Dim strCurrentTask = "Initializing"

		Try
			Dim organismName As String = String.Empty
			Dim organismNameBaseFasta As String = String.Empty

			strCurrentTask = "GetLegacyFastaFilePath for splitFastaName"
			Dim fastaFilePath = GetLegacyFastaFilePath(splitFastaName, organismName)

			If Not String.IsNullOrWhiteSpace(fastaFilePath) Then
				' Split file is defined in the database
				mErrorMessage = String.Empty
				Return True
			End If

			' Split file not found
			' Query DMS for the location of baseFastaName
			strCurrentTask = "GetLegacyFastaFilePath for baseFastaName"
			Dim baseFastaFilePath = GetLegacyFastaFilePath(baseFastaName, organismNameBaseFasta)
			If String.IsNullOrWhiteSpace(baseFastaFilePath) Then
				' Base file not found
				mErrorMessage = "Cannot find base FASTA file in DMS using V_Legacy_Static_File_Locations: " & baseFastaFilePath & "; ConnectionString: " & mProteinSeqsDBConnectionString
				OnErrorEvent(mErrorMessage)
				Return False
			End If

			Dim fiBaseFastaFile = New FileInfo(baseFastaFilePath)

			' Try to create a lock file
			Dim lockStream As FileStream
			Dim lockFilePath As String = String.Empty
			strCurrentTask = "CreateLockStream"
			lockStream = CreateLockStream(fiBaseFastaFile, lockFilePath)

			If lockStream Is Nothing Then
				' Unable to create a lock stream; an exception has likely already been thrown
				Throw New Exception("Unable to create lock file required to split " & fiBaseFastaFile.FullName)
			End If

			' Check again for the existence of the desired .Fasta file
			' It's possible another process created the .Fasta file while this process was waiting for the other process's lock file to disappear

			strCurrentTask = "GetLegacyFastaFilePath for splitFastaName (2nd time)"
			fastaFilePath = GetLegacyFastaFilePath(splitFastaName, organismName)
			If Not String.IsNullOrWhiteSpace(fastaFilePath) Then
				' The file now exists
				mErrorMessage = String.Empty
				strCurrentTask = "DeleteLockStream (fasta file now exists)"
				DeleteLockStream(lockFilePath, lockStream)
				Return True
			End If

			OnSplittingBaseFastafile(fiBaseFastaFile.FullName, mNumSplitParts)

			' Perform the splitting			
			'    Call SplitFastaFile to create a split file, using mNumSplitParts parts

			mSplitter = New clsFastaFileSplitter()
			mSplitter.ShowMessages = True
			mSplitter.LogMessagesToFile = False

			strCurrentTask = "SplitFastaFile " & fiBaseFastaFile.FullName
			Dim success = mSplitter.SplitFastaFile(fiBaseFastaFile.FullName, fiBaseFastaFile.Directory.FullName, mNumSplitParts)

			If Not success Then
				If String.IsNullOrWhiteSpace(mErrorMessage) Then
					mErrorMessage = "FastaFileSplitter returned false; unknown error"
					OnErrorEvent(mErrorMessage)
				End If
				Return False
			End If

			' Verify that the fasta files were created
			strCurrentTask = "Verify new files"
			For Each splitFileInfo In mSplitter.SplitFastaFileInfo
				Dim fiSplitFastaFile = New FileInfo(splitFileInfo.FilePath)
				If Not fiSplitFastaFile.Exists Then
					mErrorMessage = "Newly created split fasta file not found: " & splitFileInfo.FilePath
					OnErrorEvent(mErrorMessage)
					Return False
				End If
			Next

			OnProgressUpdate("Fasta file successfully split into " & mNumSplitParts & " parts", 100)

			' Store the newly created Fasta file names, plus their protein and residue stats, in DMS
			strCurrentTask = "StoreSplitFastaFileNames"
			success = StoreSplitFastaFileNames(organismNameBaseFasta, mSplitter.SplitFastaFileInfo)
			If Not success Then
				If String.IsNullOrWhiteSpace(mErrorMessage) Then
					mErrorMessage = "StoreSplitFastaFileNames returned false; unknown error"
					OnErrorEvent(mErrorMessage)
				End If
				Return False
			End If

			' Call the procedure that syncs up this information with ProteinSeqs
			strCurrentTask = "UpdateCachedOrganismDBInfo"
			UpdateCachedOrganismDBInfo()

			' Delete any cached MSGFPlus index files corresponding to the split fasta files

			If Not String.IsNullOrWhiteSpace(mMSGFPlusIndexFilesFolderPathLegacyDB) Then
				Dim diIndexFolder = New DirectoryInfo(mMSGFPlusIndexFilesFolderPathLegacyDB)

				If diIndexFolder.Exists Then

					For Each splitFileInfo In mSplitter.SplitFastaFileInfo
						Dim fileSpecBase = Path.GetFileNameWithoutExtension(splitFileInfo.FilePath)

						Dim lstFileSpecsToFind = New List(Of String)
						lstFileSpecsToFind.Add(fileSpecBase & ".*")
						lstFileSpecsToFind.Add(fileSpecBase & ".fasta.LastUsed")
						lstFileSpecsToFind.Add(fileSpecBase & ".fasta.MSGFPlusIndexFileInfo")
						lstFileSpecsToFind.Add(fileSpecBase & ".fasta.MSGFPlusIndexFileInfo.Lock")

						For Each fileSpec In lstFileSpecsToFind
							For Each fiFile As FileInfo In diIndexFolder.GetFiles(fileSpec)
								Try
									fiFile.Delete()
								Catch ex As Exception
									' Ignore errors here
								End Try
							Next
						Next
					Next

				End If

			End If

			' Delete the lock file
			strCurrentTask = "DeleteLockStream (fasta file created)"
			DeleteLockStream(lockFilePath, lockStream)

		Catch ex As Exception
			mErrorMessage = "Exception in ValidateSplitFastaFile for " + splitFastaName & " at " & strCurrentTask & ": " & ex.Message
			OnErrorEvent(mErrorMessage)
			Return False
		End Try

		Return True

	End Function

#Region "Events and Event Handlers"

	Public Event ErrorEvent(ByVal strMessage As String)
	Public Event WarningEvent(ByVal strMessage As String)
	Public Event SplittingBaseFastafile(ByVal strBaseFastaFileName As String, ByVal numSplitParts As Integer)

	Public Event ProgressUpdate(ByVal progressMessage As String, ByVal percentComplete As Integer)

	Protected Sub OnProgressUpdate(ByVal progressMessage As String, ByVal percentComplete As Integer)
		RaiseEvent ProgressUpdate(progressMessage, percentComplete)
	End Sub

	Protected Sub OnErrorEvent(ByVal errorMessageNew As String)
		RaiseEvent ErrorEvent(errorMessageNew)
	End Sub

	Protected Sub OnSplittingBaseFastafile(ByVal strBaseFastaFileName As String, ByVal numSplitParts As Integer)
		RaiseEvent SplittingBaseFastafile(strBaseFastaFileName, numSplitParts)
	End Sub

	Protected Sub OnWarningEvent(ByVal warningMessageNew As String)
		RaiseEvent WarningEvent(warningMessageNew)
	End Sub

	Private Sub mSplitter_ErrorEvent(strMessage As String) Handles mSplitter.ErrorEvent
		mErrorMessage = "Fasta Splitter Error: " & strMessage
		OnErrorEvent(strMessage)
	End Sub

	Private Sub mSplitter_WarningEvent(strMessage As String) Handles mSplitter.WarningEvent
		OnWarningEvent(strMessage)
	End Sub

	Private Sub mSplitter_ProgressChanged(taskDescription As String, percentComplete As Single) Handles mSplitter.ProgressChanged
		OnProgressUpdate(taskDescription, CInt(percentComplete))
	End Sub

#End Region

End Class

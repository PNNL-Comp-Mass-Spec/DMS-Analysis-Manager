Option Strict On

Public Class clsScanTypeFileCreator

	Protected mErrorMessage As String
	Protected mScanTypeMap As System.Collections.Generic.Dictionary(Of Integer, String)

	Protected mWorkDir As String
	Protected mDatasetName As String
	Protected mScanTypeFilePath As String
	Protected mValidScanTypeLineCount As Integer

#Region "Properties"

	Public ReadOnly Property DatasetName As String
		Get
			Return mDatasetName
		End Get
	End Property

	Public ReadOnly Property ErrorMessage As String
		Get
			Return mErrorMessage
		End Get
	End Property

	Public ReadOnly Property ScanTypeFilePath As String
		Get
			Return mScanTypeFilePath
		End Get
	End Property

	Public ReadOnly Property ValidScanTypeLineCount As Integer
		Get
			Return mValidScanTypeLineCount
		End Get
	End Property

	Public ReadOnly Property WorkDir As String
		Get
			Return mWorkDir
		End Get
	End Property

#End Region

	Public Sub New(ByVal strWorkDirectoryPath As String, ByVal strDatasetName As String)
		mWorkDir = strWorkDirectoryPath
		mDatasetName = strDatasetName
		mErrorMessage = String.Empty
		mScanTypeFilePath = String.Empty
	End Sub


	Protected Function CacheScanTypeUsingScanStatsEx(ByVal strScanStatsExFilePath As String) As Boolean

		Dim srScanStatsExFile As System.IO.StreamReader

		Dim intLinesRead As Integer
		Dim strLineIn As String
		Dim strSplitLine() As String

		Dim intScanNumberColIndex As Integer
		Dim intCollisionModeColIndex As Integer
		Dim intScanFilterColIndex As Integer

		Dim intValue As Integer
		Dim intScanNumber As Integer
		Dim strCollisionMode As String
		Dim blnStoreData As Boolean

		Try
			If mScanTypeMap Is Nothing Then
				mScanTypeMap = New System.Collections.Generic.Dictionary(Of Integer, String)
			Else
				mScanTypeMap.Clear()
			End If
			
			If Not System.IO.File.Exists(strScanStatsExFilePath) Then
				mErrorMessage = "_ScanStatsEx.txt file not found: " & strScanStatsExFilePath
				Return False
			End If

			srScanStatsExFile = New System.IO.StreamReader(New System.IO.FileStream(strScanStatsExFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))

			' Define the default column mapping
			intScanNumberColIndex = 1
			intCollisionModeColIndex = 7
			intScanFilterColIndex = 8

			intLinesRead = 0
			Do While srScanStatsExFile.Peek >= 0
				strLineIn = srScanStatsExFile.ReadLine()
				If Not String.IsNullOrEmpty(strLineIn) Then
					intLinesRead += 1
					strSplitLine = strLineIn.Split(ControlChars.Tab)

					If intLinesRead = 1 AndAlso strSplitLine.Length > 0 AndAlso Not Integer.TryParse(strSplitLine(0), intValue) Then
						' This is a header line; define the column mapping
						For intColIndex As Integer = 0 To strSplitLine.Length - 1
							Select Case strSplitLine(intColIndex).ToLower()
								Case "ScanNumber".ToLower()
									intScanNumberColIndex = intColIndex
								Case "Collision Mode".ToLower()
									intCollisionModeColIndex = intColIndex
								Case "Scan Filter Text".ToLower()
									intScanFilterColIndex = intColIndex
								Case Else
									' Ignore this column
							End Select
						Next
					Else
						' Parse out the values

						If TryGetValueInt(strSplitLine, intScanNumberColIndex, intScanNumber) Then
							strCollisionMode = String.Empty
							blnStoreData = False

							If intCollisionModeColIndex >= 0 Then
								strCollisionMode = strSplitLine(intCollisionModeColIndex)
								blnStoreData = True
							Else
								If intScanFilterColIndex >= 0 Then
									Dim strFilterText As String

									strFilterText = strSplitLine(intScanFilterColIndex)

									' Parse the filter text to determine scan type
									strCollisionMode = ThermoRawFileReaderDLL.FinniganFileIO.XRawFileIO.GetScanTypeNameFromFinniganScanFilterText(strFilterText)

									blnStoreData = True
								End If
							End If

							If blnStoreData Then
								If String.IsNullOrEmpty(strCollisionMode) Then
									strCollisionMode = "MS"
								ElseIf strCollisionMode = "0" Then
									strCollisionMode = "MS"
								End If

								If Not mScanTypeMap.ContainsKey(intScanNumber) Then
									mScanTypeMap.Add(intScanNumber, strCollisionMode)
								End If
							End If							

						End If
					End If
				End If
			Loop

			srScanStatsExFile.Close()


		Catch ex As Exception
			mErrorMessage = "Exception in CacheScanTypeUsingScanStatsEx"
			Return False
		End Try

		Return True

	End Function

	Public Function CreateScanTypeFile() As Boolean

		Dim strScanStatsFilePath As String
		Dim strScanStatsExFilePath As String

		Dim srScanStatsFile As System.IO.StreamReader
		Dim swOutFile As System.IO.StreamWriter

		Dim intLinesRead As Integer
		Dim strLineIn As String
		Dim strSplitLine() As String

		Dim intScanNumberColIndex As Integer
		Dim intScanTypeColIndex As Integer
		Dim intScanTypeNameColIndex As Integer

		Dim intValue As Integer
		Dim intScanNumber As Integer
		Dim intScanType As Integer
		Dim strScanTypeName As String

		Dim blnScanStatsExLoaded As Boolean

		Try
			mErrorMessage = String.Empty
			mValidScanTypeLineCount = 0

			strScanStatsFilePath = System.IO.Path.Combine(mWorkDir, mDatasetName & "_ScanStats.txt")
			strScanStatsExFilePath = System.IO.Path.Combine(mWorkDir, mDatasetName & "_ScanStatsEx.txt")

			If Not System.IO.File.Exists(strScanStatsFilePath) Then
				mErrorMessage = "_ScanStats.txt file not found: " & strScanStatsFilePath
				Return False
			End If

			' Open the input file
			srScanStatsFile = New System.IO.StreamReader(New System.IO.FileStream(strScanStatsFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))

			mScanTypeFilePath = System.IO.Path.Combine(mWorkDir, mDatasetName & "_ScanType.txt")

			' Create the scan type output file
			swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(mScanTypeFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

			swOutFile.WriteLine("ScanNumber" & ControlChars.Tab & "ScanTypeName" & ControlChars.Tab & "ScanType")

			' Define the default column mapping
			intScanNumberColIndex = 1
			intScanTypeColIndex = 3
			intScanTypeNameColIndex = -1

			intLinesRead = 0
			Do While srScanStatsFile.Peek >= 0
				strLineIn = srScanStatsFile.ReadLine()
				If Not String.IsNullOrEmpty(strLineIn) Then
					intLinesRead += 1
					strSplitLine = strLineIn.Split(ControlChars.Tab)

					If intLinesRead = 1 AndAlso strSplitLine.Length > 0 AndAlso Not Integer.TryParse(strSplitLine(0), intValue) Then
						' This is a header line; define the column mapping
						For intColIndex As Integer = 0 To strSplitLine.Length - 1
							Select Case strSplitLine(intColIndex).ToLower()
								Case "ScanNumber".ToLower()
									intScanNumberColIndex = intColIndex
								Case "ScanType".ToLower()
									intScanTypeColIndex = intColIndex
								Case "ScanTypeName".ToLower()
									intScanTypeNameColIndex = intColIndex
								Case Else
									' Ignore this column
							End Select
						Next
					Else
						If intScanTypeNameColIndex < 0 And Not blnScanStatsExLoaded Then
							' Need to read the ScanStatsEx file

							If Not CacheScanTypeUsingScanStatsEx(strScanStatsExFilePath) Then
								srScanStatsFile.Close()
								swOutFile.Close()
								Return False
							End If

							blnScanStatsExLoaded = True

						End If

						intScanNumber = 0
						If TryGetValueInt(strSplitLine, intScanNumberColIndex, intScanNumber) Then
							If TryGetValueInt(strSplitLine, intScanTypeColIndex, intScanType) Then

								strScanTypeName = String.Empty
								If blnScanStatsExLoaded Then
									mScanTypeMap.TryGetValue(intScanNumber, strScanTypeName)
								Else
									strScanTypeName = strSplitLine(intScanTypeNameColIndex)
								End If

								swOutFile.WriteLine(intScanNumber & ControlChars.Tab & strScanTypeName & ControlChars.Tab & intScanType)
								mValidScanTypeLineCount += 1
							End If

						End If

					End If
				End If
			Loop

			srScanStatsFile.Close()
			swOutFile.Close()

		Catch ex As Exception
			mErrorMessage = "Exception in CreateScanTypeFile"
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Tries to convert the text at index intColIndex of strData to an integer
	''' </summary>
	''' <param name="strData"></param>
	''' <param name="intColIndex"></param>
	''' <param name="intValue"></param>
	''' <returns>True if success; false if intColIndex is less than 0 or if the text cannot be converted to an integer</returns>
	''' <remarks></remarks>
	Private Function TryGetValueInt(strData() As String, intColIndex As Integer, ByRef intValue As Integer) As Boolean
		If intColIndex >= 0 AndAlso intColIndex < strData.Length Then
			If Integer.TryParse(strData(intColIndex), intValue) Then
				Return True
			End If
		End If
		Return False
	End Function

End Class

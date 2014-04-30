Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsScanTypeFileCreator

	Protected mErrorMessage As String
	Protected mExceptionDetails As String

	Protected mScanTypeMap As Dictionary(Of Integer, String)

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

	Public ReadOnly Property ExceptionDetails As String
		Get
			Return mExceptionDetails
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
		mExceptionDetails = String.Empty
		mScanTypeFilePath = String.Empty
	End Sub


	Protected Function CacheScanTypeUsingScanStatsEx(ByVal strScanStatsExFilePath As String) As Boolean

		Dim intLinesRead As Integer
		Dim strLineIn As String
		Dim strSplitLine() As String

		Dim intScanNumberColIndex As Integer
		Dim intCollisionModeColIndex As Integer
		Dim intScanFilterColIndex As Integer

		Dim intValue As Integer
		Dim intScanNumber As Integer
		Dim strCollisionMode As String
		Dim strFilterText As String

		Dim blnStoreData As Boolean

		Try
			If mScanTypeMap Is Nothing Then
				mScanTypeMap = New Dictionary(Of Integer, String)
			Else
				mScanTypeMap.Clear()
			End If
			
			If Not File.Exists(strScanStatsExFilePath) Then
				mErrorMessage = "_ScanStatsEx.txt file not found: " & strScanStatsExFilePath
				Return False
			End If

			Using srScanStatsExFile As StreamReader = New StreamReader(New FileStream(strScanStatsExFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

				' Define the default column mapping
				intScanNumberColIndex = 1
				intCollisionModeColIndex = 7
				intScanFilterColIndex = 8

				intLinesRead = 0
				Do While srScanStatsExFile.Peek > -1
					strLineIn = srScanStatsExFile.ReadLine()
					If Not String.IsNullOrEmpty(strLineIn) Then
						intLinesRead += 1
						strSplitLine = strLineIn.Split(ControlChars.Tab)

						If intLinesRead = 1 AndAlso strSplitLine.Length > 1 AndAlso Not Integer.TryParse(strSplitLine(0), intValue) Then
							' This is a header line; define the column mapping

							Const IS_CASE_SENSITIVE As Boolean = False
							Dim lstHeaderNames = New List(Of String) From {"ScanNumber", "Collision Mode", "Scan Filter Text"}

							' Keys in this dictionary are column names, values are the 0-based column index
							Dim dctHeaderMapping = clsGlobal.ParseHeaderLine(strLineIn, lstHeaderNames, IS_CASE_SENSITIVE)
							
							intScanNumberColIndex = dctHeaderMapping("ScanNumber")
							intCollisionModeColIndex = dctHeaderMapping("Collision Mode")
							intScanFilterColIndex = dctHeaderMapping("Scan Filter Text")
						
						Else
							' Parse out the values

							If TryGetValueInt(strSplitLine, intScanNumberColIndex, intScanNumber) Then
								strCollisionMode = String.Empty
								blnStoreData = False

								If TryGetValueStr(strSplitLine, intCollisionModeColIndex, strCollisionMode) Then
									blnStoreData = True
								Else
									strFilterText = String.Empty
									If TryGetValueStr(strSplitLine, intScanFilterColIndex, strFilterText) Then

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

			End Using


		Catch ex As Exception
			mErrorMessage = "Exception in CacheScanTypeUsingScanStatsEx: " & ex.GetType.Name
			mExceptionDetails = ex.Message
			Return False
		End Try

		Return True

	End Function

	Public Function CreateScanTypeFile() As Boolean

		Dim strScanStatsFilePath As String
		Dim strScanStatsExFilePath As String

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
			mExceptionDetails = String.Empty

			mValidScanTypeLineCount = 0

			strScanStatsFilePath = Path.Combine(mWorkDir, mDatasetName & "_ScanStats.txt")
			strScanStatsExFilePath = Path.Combine(mWorkDir, mDatasetName & "_ScanStatsEx.txt")

			If Not File.Exists(strScanStatsFilePath) Then
				mErrorMessage = "_ScanStats.txt file not found: " & strScanStatsFilePath
				Return False
			End If

			Dim blnDetailedScanTypesDefined = clsAnalysisResourcesMSGFDB.ValidateScanStatsFileHasDetailedScanTypes(strScanStatsFilePath)

			' Open the input file
			Using srScanStatsFile As StreamReader = New StreamReader(New FileStream(strScanStatsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

				mScanTypeFilePath = Path.Combine(mWorkDir, mDatasetName & "_ScanType.txt")

				' Create the scan type output file
				Using swOutFile As StreamWriter = New StreamWriter(New FileStream(mScanTypeFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

					swOutFile.WriteLine("ScanNumber" & ControlChars.Tab & "ScanTypeName" & ControlChars.Tab & "ScanType")

					' Define the default column mapping
					intScanNumberColIndex = 1
					intScanTypeColIndex = 3
					intScanTypeNameColIndex = -1

					intLinesRead = 0
					Do While srScanStatsFile.Peek > -1
						strLineIn = srScanStatsFile.ReadLine()
						If Not String.IsNullOrEmpty(strLineIn) Then
							intLinesRead += 1
							strSplitLine = strLineIn.Split(ControlChars.Tab)

							Dim blnFirstColumnIsNumeric = Integer.TryParse(strSplitLine(0), intValue)

							If intLinesRead = 1 AndAlso strSplitLine.Length > 0 AndAlso Not blnFirstColumnIsNumeric Then
								' This is a header line; define the column mapping

								Const IS_CASE_SENSITIVE As Boolean = False
								Dim lstHeaderNames = New List(Of String) From {"ScanNumber", "ScanType", "ScanTypeName"}

								' Keys in this dictionary are column names, values are the 0-based column index
								Dim dctHeaderMapping = clsGlobal.ParseHeaderLine(strLineIn, lstHeaderNames, IS_CASE_SENSITIVE)


								intScanNumberColIndex = dctHeaderMapping("ScanNumber")
								intScanTypeColIndex = dctHeaderMapping("ScanType")
								intScanTypeNameColIndex = dctHeaderMapping("ScanTypeName")

							ElseIf intLinesRead = 1 AndAlso blnFirstColumnIsNumeric AndAlso strSplitLine.Length >= 11 AndAlso blnDetailedScanTypesDefined Then
								' ScanStats file that does not have a header line
								' Assume the column indices are 1, 3, and 10

								intScanNumberColIndex = 1
								intScanTypeColIndex = 3
								intScanTypeNameColIndex = 10

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
										ElseIf intScanTypeNameColIndex >= 0 Then
											TryGetValueStr(strSplitLine, intScanTypeNameColIndex, strScanTypeName)
										End If

										If String.IsNullOrEmpty(strScanTypeName) Then
											strScanTypeName = "CID_Assumed"
										End If

										swOutFile.WriteLine(intScanNumber & ControlChars.Tab & strScanTypeName & ControlChars.Tab & intScanType)
										mValidScanTypeLineCount += 1
									End If

								End If

							End If
						End If
					Loop


				End Using

			End Using

		Catch ex As Exception
			mErrorMessage = "Exception in CreateScanTypeFile: " & ex.GetType.Name
			mExceptionDetails = ex.Message
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
	''' <returns>True if success; false if intColIndex is less than 0, intColIndex is out of range for strData(), or the text cannot be converted to an integer</returns>
	''' <remarks></remarks>
	Private Function TryGetValueInt(strData() As String, intColIndex As Integer, ByRef intValue As Integer) As Boolean
		If intColIndex >= 0 AndAlso intColIndex < strData.Length Then
			If Integer.TryParse(strData(intColIndex), intValue) Then
				Return True
			End If
		End If
		Return False
	End Function

	''' <summary>
	''' Tries to retrieve the string value at index intColIndex in strData()
	''' </summary>
	''' <param name="strData"></param>
	''' <param name="intColIndex"></param>
	''' <param name="strValue"></param>
	''' <returns>True if success; false if intColIndex is less than 0 or intColIndex is out of range for strData()</returns>
	''' <remarks></remarks>
	Private Function TryGetValueStr(strData() As String, intColIndex As Integer, ByRef strValue As String) As Boolean
		If intColIndex >= 0 AndAlso intColIndex < strData.Length Then
			strValue = strData(intColIndex)
			If String.IsNullOrEmpty(strValue) Then strValue = String.Empty
			Return True
		End If
		Return False
	End Function
End Class

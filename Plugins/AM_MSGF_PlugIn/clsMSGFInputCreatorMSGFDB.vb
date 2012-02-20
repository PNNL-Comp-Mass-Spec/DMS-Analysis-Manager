'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 07/20/2010
'
' This class reads a msgfdb_syn.txt file in support of creating the input file for MSGF 
'
'*********************************************************************************************************

Option Strict On

Public Class clsMSGFInputCreatorMSGFDB
	Inherits clsMSGFInputCreator

	Public Const DATA_COLUMN_ResultID As String = "ResultID"
	Public Const DATA_COLUMN_Scan As String = "Scan"
	Public Const DATA_COLUMN_FragMethod As String = "FragMethod"
	Public Const DATA_COLUMN_SpecIndex As String = "SpecIndex"
	Public Const DATA_COLUMN_Charge As String = "Charge"
	Public Const DATA_COLUMN_PrecursorMZ As String = "PrecursorMZ"
	Public Const DATA_COLUMN_DelM As String = "DelM"
	Public Const DATA_COLUMN_DelMPPM As String = "DelM_PPM"
	Public Const DATA_COLUMN_MH As String = "MH"
	Public Const DATA_COLUMN_Peptide As String = "Peptide"
	Public Const DATA_COLUMN_Protein As String = "Protein"
	Public Const DATA_COLUMN_NTT As String = "NTT"
	Public Const DATA_COLUMN_DeNovoScore As String = "DeNovoScore"
	Public Const DATA_COLUMN_MSGFScore As String = "MSGFScore"
	Public Const DATA_COLUMN_SpecProb As String = "MSGFDB_SpecProb"
	Public Const DATA_COLUMN_RankSpecProb As String = "Rank_MSGFDB_SpecProb"
	Public Const DATA_COLUMN_PValue As String = "PValue"

	Public Sub New(ByVal strDatasetName As String, _
		  ByVal strWorkDir As String, _
		  ByRef objDynamicMods As System.Collections.Generic.SortedDictionary(Of String, String), _
		  ByRef objStaticMods As System.Collections.Generic.SortedDictionary(Of String, String))

		MyBase.New(strDatasetName, strWorkDir, objDynamicMods, objStaticMods)

	End Sub

	''' <summary>
	''' Reads a MSGFDB FHT or SYN file and creates the corresponding _syn_MSGF.txt or _fht_MSGF.txt file
	''' using the MSGFDB_SpecProb values for the MSGF score
	''' </summary>
	''' <param name="strSourceFilePath"></param>
	''' <param name="strSourceFileDescription"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Function CreateMSGFFileUsingMSGFDBSpecProb(ByVal strSourceFilePath As String, strSourceFileDescription As String) As Boolean

		Dim srPHRPFile As System.IO.StreamReader
		Dim swMSGFFile As System.IO.StreamWriter

		Dim strMSGFFilePath As String

		Dim strLineIn As String
		Dim strSplitLine() As String

		Dim intLinesRead As Integer

		Dim blnSkipLine As Boolean
		Dim blnHeaderLineParsed As Boolean
		Dim blnSuccess As Boolean

		Dim udtPHRPData As udtPHRPDataLine

		Try

			If String.IsNullOrEmpty(strSourceFilePath) Then
				' Source file not defined
				mErrorMessage = "Source file not provided to CreateMSGFFileUsingMSGFDBSpecProb"
				Console.WriteLine(mErrorMessage)
				Return False
			End If

			' Open the file
			srPHRPFile = New System.IO.StreamReader(New System.IO.FileStream(strSourceFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

			' Define the path to write the first-hits MSGF results to
			strMSGFFilePath = System.IO.Path.Combine(mWorkDir, _
			 System.IO.Path.GetFileNameWithoutExtension(strSourceFilePath) & MSGF_RESULT_FILENAME_SUFFIX)

			' Create the output file
			swMSGFFile = New System.IO.StreamWriter(New System.IO.FileStream(strMSGFFilePath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))

			' Write out the headers to swMSGFFHTFile
			WriteMSGFResultsHeaders(swMSGFFile)


			intLinesRead = 0
			blnHeaderLineParsed = False

			Do While srPHRPFile.Peek >= 0
				strLineIn = srPHRPFile.ReadLine
				intLinesRead += 1
				blnSkipLine = False

				If Not String.IsNullOrEmpty(strLineIn) Then
					strSplitLine = strLineIn.Split(ControlChars.Tab)

					If Not blnHeaderLineParsed Then
						If Not clsMSGFInputCreator.IsNumber(strSplitLine(0)) Then
							' Parse the header line to confirm the column ordering
							clsMSGFInputCreator.ParseColumnHeaders(strSplitLine, mColumnHeaders)
							blnSkipLine = True
						End If

						blnHeaderLineParsed = True
					End If

					If Not blnSkipLine AndAlso strSplitLine.Length >= 4 Then

						blnSuccess = ParsePHRPDataLine(intLinesRead, strSourceFileDescription, strSplitLine, udtPHRPData)

						swMSGFFile.WriteLine(udtPHRPData.ResultID & ControlChars.Tab & _
						 udtPHRPData.ScanNumber & ControlChars.Tab & _
						 udtPHRPData.Charge & ControlChars.Tab & _
						 udtPHRPData.ProteinFirst & ControlChars.Tab & _
						 udtPHRPData.Peptide & ControlChars.Tab & _
						 udtPHRPData.SpecProb & ControlChars.Tab & _
						 String.Empty)

					End If
				End If

			Loop

			srPHRPFile.Close()
			swMSGFFile.Close()

		Catch ex As Exception
			ReportError("Error creating the MSGF file for MSGFDB file " & System.IO.Path.GetFileName(strSourceFilePath) & ": " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	Protected Overrides Sub DefineColumnHeaders()

		mColumnHeaders.Clear()

		' Define the default column mapping
		mColumnHeaders.Add(DATA_COLUMN_ResultID, 0)
		mColumnHeaders.Add(DATA_COLUMN_Scan, 1)
		mColumnHeaders.Add(DATA_COLUMN_FragMethod, 2)
		mColumnHeaders.Add(DATA_COLUMN_SpecIndex, 3)
		mColumnHeaders.Add(DATA_COLUMN_Charge, 4)
		mColumnHeaders.Add(DATA_COLUMN_PrecursorMZ, 5)
		mColumnHeaders.Add(DATA_COLUMN_DelM, 6)
		mColumnHeaders.Add(DATA_COLUMN_DelMPPM, 7)
		mColumnHeaders.Add(DATA_COLUMN_MH, 8)
		mColumnHeaders.Add(DATA_COLUMN_Peptide, 9)
		mColumnHeaders.Add(DATA_COLUMN_Protein, 10)
		mColumnHeaders.Add(DATA_COLUMN_NTT, 11)
		mColumnHeaders.Add(DATA_COLUMN_DeNovoScore, 12)
		mColumnHeaders.Add(DATA_COLUMN_MSGFScore, 13)
		mColumnHeaders.Add(DATA_COLUMN_SpecProb, 14)
		mColumnHeaders.Add(DATA_COLUMN_RankSpecProb, 15)
		mColumnHeaders.Add(DATA_COLUMN_PValue, 16)

	End Sub

	Public Shared Function GetPHRPFirstHitsFileName(ByVal strDatasetName As String) As String
		Return strDatasetName & "_msgfdb_fht.txt"
	End Function

	Public Shared Function GetPHRPSynopsisFileName(ByVal strDatasetName As String) As String
		Return strDatasetName & "_msgfdb_syn.txt"
	End Function

	Public Shared Function GetPHRPResultToSeqMapFileName(ByVal strDatasetName As String) As String
		Return strDatasetName & "_msgfdb_syn_ResultToSeqMap.txt"
	End Function

	Public Shared Function GetPHRPSeqToProteinMapFileName(ByVal strDatasetName As String) As String
		Return strDatasetName & "_msgfdb_syn_SeqToProteinMap.txt"
	End Function

	Protected Overrides Sub InitializeFilePaths()

		' Customize mPHRPResultFilePath for MSGFDB synopsis files
		mPHRPFirstHitsFilePath = System.IO.Path.Combine(mWorkDir, GetPHRPFirstHitsFileName(mDatasetName))
		mPHRPSynopsisFilePath = System.IO.Path.Combine(mWorkDir, GetPHRPSynopsisFileName(mDatasetName))

		UpdateMSGFInputOutputFilePaths()

	End Sub

	Protected Overrides Function ParsePHRPDataLine(ByVal intLineNumber As Integer, _
		 ByRef strPHRPSource As String, _
		 ByRef strColumns() As String, _
		 ByRef udtPHRPData As udtPHRPDataLine) As Boolean
		Dim blnSuccess As Boolean

		Try

			udtPHRPData.Clear()

			With udtPHRPData
				.Title = String.Empty

				.ScanNumber = LookupColumnValue(strColumns, DATA_COLUMN_Scan, mColumnHeaders, -100)
				If .ScanNumber = -100 Then
					' Data line is not valid
				Else
					.Peptide = LookupColumnValue(strColumns, DATA_COLUMN_Peptide, mColumnHeaders)
					.Charge = CType(LookupColumnValue(strColumns, DATA_COLUMN_Charge, mColumnHeaders, 0), Short)
					.ProteinFirst = LookupColumnValue(strColumns, DATA_COLUMN_Protein, mColumnHeaders)
					.ResultID = LookupColumnValue(strColumns, DATA_COLUMN_ResultID, mColumnHeaders, 0)
					.CollisionMode = LookupColumnValue(strColumns, DATA_COLUMN_FragMethod, mColumnHeaders, "n/a")
					.SpecProb = LookupColumnValue(strColumns, DATA_COLUMN_SpecProb, mColumnHeaders, "1")

					.PassesFilters = True

					blnSuccess = True
				End If
			End With

		Catch ex As Exception
			MyBase.ReportError("Error parsing line " & intLineNumber & " in the MSGFDB PHRP " & strPHRPSource & " file: " & ex.Message)
		End Try

		Return blnSuccess

	End Function

End Class

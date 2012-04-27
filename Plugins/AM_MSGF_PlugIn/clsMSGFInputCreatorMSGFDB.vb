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

	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="strDatasetName">Dataset name</param>
	''' <param name="strWorkDir">Working directory</param>
	''' <remarks></remarks>
	Public Sub New(ByVal strDatasetName As String, ByVal strWorkDir As String)

		MyBase.New(strDatasetName, strWorkDir, PHRPReader.clsPHRPReader.ePeptideHitResultType.MSGFDB)

	End Sub

	Protected Overrides Sub InitializeFilePaths()

		' Customize mPHRPResultFilePath for MSGFDB synopsis files
		mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, PHRPReader.clsPHRPParserMSGFDB.GetPHRPFirstHitsFileName(mDatasetName))
		mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, PHRPReader.clsPHRPParserMSGFDB.GetPHRPSynopsisFileName(mDatasetName))

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

		Dim strMSGFFilePath As String

		Try

			If String.IsNullOrEmpty(strSourceFilePath) Then
				' Source file not defined
				mErrorMessage = "Source file not provided to CreateMSGFFileUsingMSGFDBSpecProb"
				Console.WriteLine(mErrorMessage)
				Return False
			End If


			' Open the file (no need to read the Mods and Seq Info since we're not actually running MSGF)
			Using objReader As PHRPReader.clsPHRPReader = New PHRPReader.clsPHRPReader(strSourceFilePath, PHRPReader.clsPHRPReader.ePeptideHitResultType.MSGFDB, blnLoadModsAndSeqInfo:=False, blnLoadMSGFResults:=False)
				objReader.SkipDuplicatePSMs = False

				' Define the path to write the first-hits MSGF results to
				strMSGFFilePath = System.IO.Path.Combine(mWorkDir, _
				 System.IO.Path.GetFileNameWithoutExtension(strSourceFilePath) & MSGF_RESULT_FILENAME_SUFFIX)

				' Create the output file
				Using swMSGFFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strMSGFFilePath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))

					' Write out the headers to swMSGFFHTFile
					WriteMSGFResultsHeaders(swMSGFFile)

					Do While objReader.MoveNext()

						Dim objPSM As PHRPReader.clsPSM
						objPSM = objReader.CurrentPSM

						Dim strSpecProb As String
						strSpecProb = objPSM.GetScore(PHRPReader.clsPHRPParserMSGFDB.DATA_COLUMN_MSGFDB_SpecProb)

						swMSGFFile.WriteLine( _
						   objPSM.ResultID & ControlChars.Tab & _
						   objPSM.ScanNumber & ControlChars.Tab & _
						   objPSM.Charge & ControlChars.Tab & _
						   objPSM.ProteinFirst & ControlChars.Tab & _
						   objPSM.Peptide & ControlChars.Tab & _
						   strSpecProb & ControlChars.Tab & _
						   String.Empty)
					Loop

				End Using

			End Using

		Catch ex As Exception
			ReportError("Error creating the MSGF file for MSGFDB file " & System.IO.Path.GetFileName(strSourceFilePath) & ": " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	Protected Overrides Function PassesFilters(ByRef objPSM As PHRPReader.clsPSM) As Boolean
		Dim blnPassesFilters As Boolean

		' All MSGFDB data is considered to be "filter-passing"
		blnPassesFilters = True

		Return blnPassesFilters

	End Function

End Class

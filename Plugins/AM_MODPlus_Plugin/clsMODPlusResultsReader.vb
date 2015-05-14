Imports System.IO
Imports System.Text.RegularExpressions

Public Class clsMODPlusResultsReader

    Public ReadOnly Property CurrentScanData As List(Of String)
        Get
            Return mCurrentScanData
        End Get
    End Property

    ''' <summary>
    ''' Currently available scan number
    ''' </summary>
    ''' <remarks>-1 if no more scans remain</remarks>
    Public ReadOnly Property CurrentScan As Integer
        Get
            Return mCurrentScan
        End Get
    End Property

    Public ReadOnly Property ResultFile As FileInfo
        Get
            Return mResultFile
        End Get
    End Property

    Public ReadOnly Property SpectrumAvailable As Boolean
        Get
            Return mSpectrumAvailable
        End Get
    End Property


    Protected mCurrentScan As Integer
    Protected mCurrentScanData As List(Of String)
    Protected mSavedLine As String
    Protected mSpectrumAvailable As Boolean

    Protected ReadOnly mExtractScanNum As Regex

    Protected ReadOnly mReader As StreamReader
    Protected ReadOnly mResultFile As FileInfo


    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub New(modPlusResultsFile As FileInfo)

        mResultFile = modPlusResultsFile

        mExtractScanNum = New Regex("\.mgf\t(\d+)", RegexOptions.Compiled Or RegexOptions.IgnoreCase)

        mReader = New StreamReader(New FileStream(modPlusResultsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

        mCurrentScanData = New List(Of String)
        mSavedLine = String.Empty

        ReadNextSpectrum()

    End Sub

    Public Function ReadNextSpectrum() As Boolean

        mSpectrumAvailable = False
        mCurrentScan = -1

        If mReader.EndOfStream Then
            Return False
        End If

        Dim dataLine As String
        If Not String.IsNullOrEmpty(mSavedLine) Then
            dataLine = String.Copy(mSavedLine)
            mSavedLine = String.Empty
        Else
            dataLine = mReader.ReadLine()
        End If

        mCurrentScanData.Clear()

        Dim startScanFound = False

        While True

            If Not String.IsNullOrWhiteSpace(dataLine) Then
                If dataLine.StartsWith(">>") Then
                    If startScanFound Then
                        ' This is the second time we've encountered ">>" in this function
                        ' Cache the line so it can be used the next time ReadNextSpectrum is called
                        mSavedLine = dataLine
                        mSpectrumAvailable = True
                        Return True
                    End If

                    startScanFound = True

                    Dim reMatch As Match = mExtractScanNum.Match(dataLine)
                    mCurrentScan = 0

                    If reMatch.Success Then
                        Integer.TryParse(reMatch.Groups(1).Value, mCurrentScan)
                    End If

                End If
                mCurrentScanData.Add(dataLine)
            End If

            If mReader.EndOfStream Then
                If mCurrentScanData.Count > 0 Then
                    mSpectrumAvailable = True
                    Return True
                Else
                    Return False
                End If
            End If

            dataLine = mReader.ReadLine()

        End While

        ' ReSharper disable once VbUnreachableCode
        Return False

    End Function
End Class

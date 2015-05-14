Imports System.IO

''' <summary>
''' This class splits a Mascot Generic File (mgf file) into multiple parts
''' </summary>
''' <remarks></remarks>
Public Class clsSplitMGFFile
    Inherits clsEventNotifier

    Protected Structure udtOutputFileType
        Public OutputFile As FileInfo
        Public SpectraWritten As Integer
        Public Writer As StreamWriter
    End Structure

    ''' <summary>
    ''' Splits a Mascot Generic File (mgf file) into splitCount parts
    ''' </summary>
    ''' <param name="mgfFilePath">.mgf file path</param>
    ''' <param name="splitCount">Number of parts; minimum is 2</param>
    ''' <returns>True if success, False is an error</returns>
    ''' <remarks>Exceptions will be reported using event ErrorEvent</remarks>
    Public Function SplitMgfFile(mgfFilePath As String, splitCount As Integer) As List(Of FileInfo)
        Return SplitMgfFile(mgfFilePath, splitCount, "_Part")
    End Function

    ''' <summary>
    ''' Splits a Mascot Generic File (mgf file) into splitCount parts
    ''' </summary>
    ''' <param name="mgfFilePath">.mgf file path</param>
    ''' <param name="splitCount">Number of parts; minimum is 2</param>
    ''' <param name="fileSuffix">Text to append to each split file (just before the file extension)</param>
    ''' <returns>List of split files if success; empty list if an error</returns>
    ''' <remarks>Exceptions will be reported using event ErrorEvent</remarks>
    Public Function SplitMgfFile(mgfFilePath As String, splitCount As Integer, fileSuffix As String) As List(Of FileInfo)

        Try

            If String.IsNullOrWhiteSpace(fileSuffix) Then
                fileSuffix = "_Part"
            End If

            Dim fiMgfFile = New FileInfo(mgfFilePath)
            If Not fiMgfFile.Exists Then
                OnErrorEvent("File not found: " & mgfFilePath)
                Return New List(Of FileInfo)
            End If

            If Not fiMgfFile.Length = 0 Then
                OnErrorEvent("MGF file is empty: " & mgfFilePath)
                Return New List(Of FileInfo)
            End If

            If splitCount < 2 Then splitCount = 2
            Dim dtLastProgress = DateTime.UtcNow

            OnProgressUpdate("Splitting " & fiMgfFile.Name & " into " & splitCount & " parts", 0)

            Dim lstSplitMgfFiles = New List(Of FileInfo)

            Using srMgfFile = New StreamReader(New FileStream(fiMgfFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                ' Create the writers
                ' Keys are each StreamWriter, values are the number of spectra written to the file
                Dim swWriters = New Queue(Of udtOutputFileType)

                For splitFile = 1 To splitCount
                    Dim outputFilePath = Path.Combine(fiMgfFile.DirectoryName, Path.GetFileNameWithoutExtension(fiMgfFile.Name) & fileSuffix & splitFile & ".mgf")

                    Dim nextWriter = New udtOutputFileType()
                    nextWriter.OutputFile = New FileInfo(outputFilePath)
                    nextWriter.SpectraWritten = 0

                    lstSplitMgfFiles.Add(nextWriter.OutputFile)
                    nextWriter.Writer = New StreamWriter(New FileStream(nextWriter.OutputFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read))
                    
                    swWriters.Enqueue(nextWriter)
                Next

                Dim bytesRead As Int64 = 0

                Dim previousLine As String = String.Empty
                While True
                    Dim spectrumData As List(Of String) = GetNextMGFSpectrum(srMgfFile, previousLine, bytesRead)
                    If spectrumData.Count = 0 Then
                        Exit While
                    Else
                        Dim nextWriter = swWriters.Dequeue()
                        For Each dataLine In spectrumData
                            nextWriter.Writer.WriteLine(dataLine)
                        Next

                        nextWriter.SpectraWritten += 1
                        swWriters.Enqueue(nextWriter)
                    End If

                    If DateTime.UtcNow.Subtract(dtLastProgress).TotalSeconds >= 5 Then
                        dtLastProgress = DateTime.UtcNow
                        Dim percentComplete = bytesRead / srMgfFile.BaseStream.Length * 100
                        If percentComplete > 100 Then percentComplete = 100
                        OnProgressUpdate("Splitting MGF file", CInt(percentComplete))
                    End If
                End While

                ' Close the writers
                ' In addition, delete any output files that did not have any spectra written to them
                Dim totalSpectraWritten = 0

                While swWriters.Count > 0
                    Dim nextWriter = swWriters.Dequeue()
                    nextWriter.Writer.Close()

                    If nextWriter.SpectraWritten = 0 Then
                        Threading.Thread.Sleep(50)
                        nextWriter.OutputFile.Delete()
                        lstSplitMgfFiles.Remove(nextWriter.OutputFile)
                    Else
                        totalSpectraWritten += nextWriter.SpectraWritten
                    End If

                End While

                If totalSpectraWritten = 0 Then
                    OnErrorEvent("No spectra were read from the source MGF file (BEGIN IONS not found)")
                    Return New List(Of FileInfo)
                End If
            End Using


            Return lstSplitMgfFiles

        Catch ex As Exception
            OnErrorEvent("Error in SplitMgfFile: " & ex.Message)
            Return New List(Of FileInfo)
        End Try

    End Function

    Private Function GetNextMGFSpectrum(srMgfFile As StreamReader, ByRef previousLine As String, ByRef bytesRead As Int64) As List(Of String)

        Dim spectrumFound = False
        Dim spectrumData = New List(Of String)

        If srMgfFile.EndOfStream Then Return spectrumData

        Dim dataLine As String
        If String.IsNullOrWhiteSpace(previousLine) Then
            dataLine = srMgfFile.ReadLine()
            bytesRead += 2
        Else
            dataLine = String.Copy(previousLine)
            previousLine = String.Empty
        End If

        While True

            If Not String.IsNullOrWhiteSpace(dataLine) Then
                bytesRead += dataLine.Length

                If dataLine.ToUpper().StartsWith("BEGIN IONS") Then
                    If spectrumFound Then
                        ' The previous spectrum was missing the END IONS line
                        ' This is unexpected, but we'll allow it
                        previousLine = dataLine

                        spectrumData.Add("END IONS")
                        Return spectrumData
                    End If
                    spectrumFound = True
                End If

                If spectrumFound Then
                    spectrumData.Add(dataLine)
                End If

                If dataLine.ToUpper().StartsWith("END IONS") Then
                    Return spectrumData
                End If
            End If

            If srMgfFile.EndOfStream Then Exit While
            dataLine = srMgfFile.ReadLine()
            bytesRead += 2
        End While

        Return spectrumData

    End Function

End Class

using PdfSharp;
using PdfSharp.Drawing;
using PdfSharp.Pdf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace AnalysisManagerFormularityPlugin
{
    public class PngToPdfConverter : PRISM.EventNotifier
    {
        // Ignore Spelling: formularity, Pdf, Png

        /// <summary>
        /// Page margin, in points
        /// </summary>
        /// <remarks>
        /// For reference: 72 points is 1 inch, as defined by the DTP (or PostScript) point.
        /// </remarks>
        private const double PageMargin = 36;

        /// <summary>
        /// Double the page margin
        /// </summary>
        private readonly XUnit DoublePageMargin = XUnit.FromPoint(PageMargin * 2);

        private readonly XFont mFontHeader;

        private readonly XFont mFontDefault;

        /// <summary>
        /// Dataset name
        /// </summary>
        private string DatasetName { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName"></param>
        public PngToPdfConverter(string datasetName)
        {
            DatasetName = datasetName;

            const string FONT_FAMILY = "Arial";
            mFontHeader = new XFont(FONT_FAMILY, 20, XFontStyleEx.Regular);
            mFontDefault = new XFont(FONT_FAMILY, 10, XFontStyleEx.Regular);
        }

        /// <summary>
        /// Add a new page with landscape orientation
        /// </summary>
        /// <param name="pdfDoc"></param>
        private PdfPageInfo AddPage(PdfDocument pdfDoc)
        {
            var currentPageInfo = new PdfPageInfo(pdfDoc, PageMargin, PageOrientation.Landscape);
            return currentPageInfo;
        }

        /// <summary>
        /// Adds a plot image
        /// </summary>
        /// <param name="currentPage">Current PDF page</param>
        /// <param name="plotImage">Plot image</param>
        /// <param name="xOffset">X offset</param>
        /// <param name="width">Plot width, in points</param>
        /// <param name="height">Plot height, in points</param>
        /// <param name="header">Header (optional)</param>
        private void AddPlot(
            PdfPageInfo currentPage, XImage plotImage, double xOffset,
            double width, double height, string header = null)
        {
            var y = currentPage.CurrentPageY;

            if (!string.IsNullOrWhiteSpace(header))
            {
                // Add a header over the image
                var yOffsetIncrement = AddText(currentPage, header, mFontDefault, xOffset: xOffset);
                y += yOffsetIncrement - 10;
            }

            currentPage.PageGraphics.DrawImage(plotImage, new XRect(xOffset, y, width, height));
            plotImage.Dispose();
        }

        /// <summary>
        /// Add text
        /// </summary>
        /// <param name="currentPage">Current page</param>
        /// <param name="text">Text to add</param>
        /// <param name="font">Text font</param>
        /// <param name="yScalar">Y scalar; 0 or 1 means full size text</param>
        /// <param name="xOffset">X offset; if 0, will be at the left page margin</param>
        /// <param name="width">Textbox width; if 0 or larger than the page width, will center the text on the page</param>
        /// <param name="position">Position of the text in the textbox</param>
        private double AddText(
            PdfPageInfo currentPage, string text, XFont font,
            double yScalar = 0, double xOffset = 0,
            double width = -1, XStringFormat position = null)
        {
            var textHeight = GetTextHeight(text, currentPage.PageGraphics, font);
            var y = currentPage.CurrentPageY;

            if (Math.Abs(yScalar) > 0)
            {
                y += textHeight * yScalar;
            }

            double x;

            if (xOffset < PageMargin || XUnit.FromPoint(xOffset) > currentPage.Page.Width - XUnit.FromPoint(PageMargin))
            {
                x = PageMargin;
            }
            else
            {
                x = xOffset;
            }

            if (width < x || XUnit.FromPoint(width) > currentPage.Page.Width - DoublePageMargin)
            {
                width = (currentPage.Page.Width - DoublePageMargin).Point;
            }

            position ??= XStringFormats.Default;

            var height = textHeight;

            if (position.LineAlignment == XLineAlignment.BaseLine)
            {
                height = 0;
            }

            currentPage.PageGraphics.DrawString(text, font, XBrushes.Black, new XRect(x, y, width, height), position);

            // Return the effective text height after adjustments
            // This value is used to increment currentPage.CurrentPageY
            return y - currentPage.CurrentPageY + textHeight;
        }

        /// <summary>
        /// Create a PDF file with the given PNG files
        /// </summary>
        /// <param name="pdfFilePath">Path to the PDF file create</param>
        /// <param name="pngFiles"></param>
        /// <param name="dataSource"></param>
        public bool CreatePdf(string pdfFilePath, List<FileInfo> pngFiles, string dataSource = "NOMSI")
        {
            if (string.IsNullOrWhiteSpace(dataSource))
            {
                dataSource = "Unknown_Source";
            }

            try
            {
                var pdfFile = new FileInfo(pdfFilePath);

                if (pdfFile.Exists)
                    pdfFile.Delete();

                var pdfDoc = new PdfDocument();
                pdfDoc.Options.NoCompression = true;
                pdfDoc.PageMode = PdfPageMode.UseNone;

                // Get the plot layout

                const string datasetDetailReportLink = "";
                var pngFileTableLayout = GetPngFileTableLayout(DatasetName, datasetDetailReportLink);

                var pngFileNames = new SortedSet<string>();

                foreach (var item in pngFiles)
                {
                    pngFileNames.Add(item.Name);
                }

                var workDir = pngFiles.First().DirectoryName;

                if (string.IsNullOrWhiteSpace(workDir))
                {
                    OnErrorEvent("Cannot determine the parent directory of " + pngFiles.First());
                    return false;
                }

                // Spacing between plots on the same row
                const int PLOT_SPACING_X = 10;

                const int ROWS_PER_AGE = 2;
                var rowsProcessed = 0;

                PdfPageInfo currentPage = null;

                // Add each of the plots
                foreach (var tableRow in pngFileTableLayout)
                {
                    if (currentPage == null || rowsProcessed % ROWS_PER_AGE == 0)
                    {
                        // Create a new page
                        currentPage = AddPage(pdfDoc);

                        // Include the Dataset name as the header on every page
                        const double yScalar = 0.75;
                        var yOffsetIncrement = AddText(currentPage, DatasetName, mFontHeader, yScalar, position: XStringFormats.Center);
                        currentPage.IncrementY(yOffsetIncrement);

                        // Add 10 points of vertical whitespace
                        currentPage.IncrementY(10);
                    }

                    // Scaled plot width, in points
                    var plotWidth = (currentPage.Page.Width - DoublePageMargin - XUnit.FromPoint(10)) / 2;
                    var xOffset = PageMargin;
                    double yOffsetIncrementForRow = 0;

                    foreach (var tableCell in tableRow)
                    {
                        if (!string.IsNullOrWhiteSpace(tableCell))
                        {
                            if (pngFileNames.Contains(tableCell))
                            {
                                var pngFileName = tableCell;

                                var plotImage = XImage.FromFile(Path.Combine(workDir, pngFileName));

                                // Scaled plot height, in points
                                var plotHeight = plotImage.PointHeight / plotImage.PointWidth * plotWidth.Point;

                                // Note that AddPlot will call plotImage.Dispose
                                AddPlot(currentPage, plotImage, xOffset, plotWidth.Point, plotHeight, "");
                                xOffset += plotWidth.Point + 5;

                                yOffsetIncrementForRow = Math.Max(yOffsetIncrementForRow, plotHeight);
                            }
                            else
                            {
                                var yOffsetIncrement = AddText(currentPage, string.Format("File not found: {0}", tableCell), mFontDefault, 0, xOffset);

                                yOffsetIncrementForRow = Math.Max(yOffsetIncrementForRow, yOffsetIncrement);
                            }
                        }

                        xOffset += PLOT_SPACING_X;
                    }

                    currentPage.IncrementY(yOffsetIncrementForRow);

                    // Add 10 points of vertical whitespace
                    currentPage.IncrementY(10);

                    rowsProcessed++;
                }

                pdfDoc.Save(pdfFile.FullName);

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error creating the PDF using .png files from " + dataSource, ex);
                return false;
            }
        }

        /// <summary>
        /// Get a list of lists that describes how to arrange the PNG files in the PDF file
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="datasetDetailReportLink">Text to display instead of a PNG file</param>
        public static List<List<string>> GetPngFileTableLayout(string datasetName, string datasetDetailReportLink)
        {
            // PNG filename suffix
            var suffix = "_" + datasetName + ".png";

            // This tracks the PNG file names that will be added to the PDF file
            // There are two PNG images in each row
            // Use "text: " to instead include literal text instead of a PNG file
            var tableLayoutByRow = new List<List<string>>
            {
                new() {"EC_count" + suffix, "KMD1_assigned" + suffix},
                new() {"vK" + suffix, "KMD1_unassigned" + suffix},

                new() {"histA" + suffix, "mErr" + suffix},
                new() {"histM" + suffix, datasetDetailReportLink},

                new() {"Ox" + suffix, "OxN" + suffix},
                new() {"OxS" + suffix, "OxP" + suffix}
            };

            return tableLayoutByRow;
        }

        /// <summary>
        /// Determine the height of the given text, in points
        /// </summary>
        /// <param name="text"></param>
        /// <param name="gfx"></param>
        /// <param name="font"></param>
        private static double GetTextHeight(string text, XGraphics gfx, XFont font)
        {
            var textSize = gfx.MeasureString(text, font);
            return textSize.Height;
        }
    }
}

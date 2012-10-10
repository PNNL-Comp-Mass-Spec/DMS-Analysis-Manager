namespace MageFactors {
    partial class Form1 {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing) {
            if (disposing && (components != null)) {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent() {
            this.panel1 = new System.Windows.Forms.Panel();
            this.listDisplayControl1 = new MageClientLib.ListDisplayControl();
            this.datasetListPanel1 = new MageClientLib.DatasetListPanel();
            this.statusPanel1 = new MageClientLib.StatusPanel();
            this.sqLiteOutputPanel1 = new MageClientLib.SQLiteOutputPanel();
            this.panel1.SuspendLayout();
            this.SuspendLayout();
            // 
            // panel1
            // 
            this.panel1.Controls.Add(this.statusPanel1);
            this.panel1.Controls.Add(this.sqLiteOutputPanel1);
            this.panel1.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel1.Location = new System.Drawing.Point(0, 767);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(852, 109);
            this.panel1.TabIndex = 5;
            // 
            // listDisplayControl1
            // 
            this.listDisplayControl1.Accumulator = null;
            this.listDisplayControl1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.listDisplayControl1.Location = new System.Drawing.Point(0, 122);
            this.listDisplayControl1.Name = "listDisplayControl1";
            this.listDisplayControl1.Notice = "";
            this.listDisplayControl1.PageTitle = "Title";
            this.listDisplayControl1.Size = new System.Drawing.Size(852, 645);
            this.listDisplayControl1.TabIndex = 2;
            // 
            // datasetListPanel1
            // 
            this.datasetListPanel1.Dock = System.Windows.Forms.DockStyle.Top;
            this.datasetListPanel1.Location = new System.Drawing.Point(0, 0);
            this.datasetListPanel1.Name = "datasetListPanel1";
            this.datasetListPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.datasetListPanel1.Size = new System.Drawing.Size(852, 122);
            this.datasetListPanel1.TabIndex = 3;
            // 
            // statusPanel1
            // 
            this.statusPanel1.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.statusPanel1.Location = new System.Drawing.Point(0, 67);
            this.statusPanel1.Name = "statusPanel1";
            this.statusPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.statusPanel1.Size = new System.Drawing.Size(852, 42);
            this.statusPanel1.TabIndex = 1;
            // 
            // sqLiteOutputPanel1
            // 
            this.sqLiteOutputPanel1.DatabaseName = "C:\\Data\\test.db";
            this.sqLiteOutputPanel1.Dock = System.Windows.Forms.DockStyle.Top;
            this.sqLiteOutputPanel1.Location = new System.Drawing.Point(0, 0);
            this.sqLiteOutputPanel1.Name = "sqLiteOutputPanel1";
            this.sqLiteOutputPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.sqLiteOutputPanel1.Size = new System.Drawing.Size(852, 70);
            this.sqLiteOutputPanel1.TabIndex = 4;
            this.sqLiteOutputPanel1.TableName = "DMS_Factors";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(852, 876);
            this.Controls.Add(this.listDisplayControl1);
            this.Controls.Add(this.datasetListPanel1);
            this.Controls.Add(this.panel1);
            this.Name = "Form1";
            this.Text = "Mage Factors";
            this.panel1.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private MageClientLib.StatusPanel statusPanel1;
        private MageClientLib.ListDisplayControl listDisplayControl1;
        private MageClientLib.DatasetListPanel datasetListPanel1;
        private MageClientLib.SQLiteOutputPanel sqLiteOutputPanel1;
        private System.Windows.Forms.Panel panel1;
    }
}


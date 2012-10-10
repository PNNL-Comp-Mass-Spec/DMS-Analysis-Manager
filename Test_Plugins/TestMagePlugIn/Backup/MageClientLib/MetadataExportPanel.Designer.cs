namespace MageClientLib {
    partial class MetadataExportPanel {
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

        #region Component Designer generated code

        /// <summary> 
        /// Required method for Designer support - do not modify 
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent() {
            this.panel2 = new System.Windows.Forms.Panel();
            this.label4 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.OutputTableNameCtl = new System.Windows.Forms.TextBox();
            this.OutputFilePathCtl = new System.Windows.Forms.TextBox();
            this.OutputTypeCtl = new System.Windows.Forms.ComboBox();
            this.label2 = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.FormatTypeCtl = new System.Windows.Forms.ComboBox();
            this.button2 = new System.Windows.Forms.Button();
            this.button1 = new System.Windows.Forms.Button();
            this.panel2.SuspendLayout();
            this.SuspendLayout();
            // 
            // panel2
            // 
            this.panel2.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.panel2.Controls.Add(this.button1);
            this.panel2.Controls.Add(this.label4);
            this.panel2.Controls.Add(this.label3);
            this.panel2.Controls.Add(this.OutputTableNameCtl);
            this.panel2.Controls.Add(this.OutputFilePathCtl);
            this.panel2.Controls.Add(this.OutputTypeCtl);
            this.panel2.Controls.Add(this.label2);
            this.panel2.Controls.Add(this.label1);
            this.panel2.Controls.Add(this.FormatTypeCtl);
            this.panel2.Controls.Add(this.button2);
            this.panel2.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel2.Location = new System.Drawing.Point(5, 5);
            this.panel2.Name = "panel2";
            this.panel2.Size = new System.Drawing.Size(926, 70);
            this.panel2.TabIndex = 7;
            // 
            // label4
            // 
            this.label4.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(534, 10);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(132, 13);
            this.label4.TabIndex = 12;
            this.label4.Text = "Output Table (SQLite only)";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(3, 10);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(83, 13);
            this.label3.TabIndex = 11;
            this.label3.Text = "Output File Path";
            // 
            // OutputTableNameCtl
            // 
            this.OutputTableNameCtl.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.OutputTableNameCtl.Location = new System.Drawing.Point(673, 7);
            this.OutputTableNameCtl.Name = "OutputTableNameCtl";
            this.OutputTableNameCtl.Size = new System.Drawing.Size(248, 20);
            this.OutputTableNameCtl.TabIndex = 10;
            // 
            // OutputFilePathCtl
            // 
            this.OutputFilePathCtl.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.OutputFilePathCtl.Location = new System.Drawing.Point(101, 7);
            this.OutputFilePathCtl.Name = "OutputFilePathCtl";
            this.OutputFilePathCtl.Size = new System.Drawing.Size(385, 20);
            this.OutputFilePathCtl.TabIndex = 10;
            this.OutputFilePathCtl.Text = "C:\\Data\\Factors.txt";
            // 
            // OutputTypeCtl
            // 
            this.OutputTypeCtl.FormattingEnabled = true;
            this.OutputTypeCtl.Items.AddRange(new object[] {
            "Tab-Delimited File",
            "SQLite Database"});
            this.OutputTypeCtl.Location = new System.Drawing.Point(335, 36);
            this.OutputTypeCtl.Name = "OutputTypeCtl";
            this.OutputTypeCtl.Size = new System.Drawing.Size(121, 21);
            this.OutputTypeCtl.TabIndex = 9;
            this.OutputTypeCtl.Text = "Tab-Delimited File";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(263, 39);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(66, 13);
            this.label2.TabIndex = 8;
            this.label2.Text = "Output Type";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(3, 39);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(74, 13);
            this.label1.TabIndex = 8;
            this.label1.Text = "Output Format";
            // 
            // FormatTypeCtl
            // 
            this.FormatTypeCtl.FormattingEnabled = true;
            this.FormatTypeCtl.Items.AddRange(new object[] {
            "Factors",
            "Dataset Metadata"});
            this.FormatTypeCtl.Location = new System.Drawing.Point(101, 36);
            this.FormatTypeCtl.Name = "FormatTypeCtl";
            this.FormatTypeCtl.Size = new System.Drawing.Size(121, 21);
            this.FormatTypeCtl.TabIndex = 7;
            this.FormatTypeCtl.Text = "Factors";
            this.FormatTypeCtl.SelectedIndexChanged += new System.EventHandler(this.FormatTypeCtl_SelectedIndexChanged);
            // 
            // button2
            // 
            this.button2.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.button2.Location = new System.Drawing.Point(831, 32);
            this.button2.Name = "button2";
            this.button2.Size = new System.Drawing.Size(90, 26);
            this.button2.TabIndex = 6;
            this.button2.Text = "Save";
            this.button2.UseVisualStyleBackColor = true;
            this.button2.Click += new System.EventHandler(this.button2_Click);
            // 
            // button1
            // 
            this.button1.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.button1.Location = new System.Drawing.Point(492, 5);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(30, 23);
            this.button1.TabIndex = 13;
            this.button1.Text = "...";
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Click += new System.EventHandler(this.button1_Click);
            // 
            // MetadataExportPanel
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.panel2);
            this.Name = "MetadataExportPanel";
            this.Padding = new System.Windows.Forms.Padding(5);
            this.Size = new System.Drawing.Size(936, 80);
            this.panel2.ResumeLayout(false);
            this.panel2.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Panel panel2;
        private System.Windows.Forms.ComboBox FormatTypeCtl;
        private System.Windows.Forms.Button button2;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox OutputTableNameCtl;
        private System.Windows.Forms.TextBox OutputFilePathCtl;
        private System.Windows.Forms.ComboBox OutputTypeCtl;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button button1;
    }
}

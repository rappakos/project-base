"""
Export synthetic queries to Excel for review.

Exports all queries (real requirements and generated synthetic queries) 
to an Excel file with project context for manual review.
"""

import asyncio
import pandas as pd
from datetime import datetime
import aiosqlite

import config


async def export_queries_to_excel(output_file: str = None):
    """
    Export all queries to Excel with project context.
    
    Args:
        output_file: Path to output Excel file (default: queries_YYYYMMDD_HHMMSS.xlsx)
    """
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"queries_{timestamp}.xlsx"
    
    conn = await aiosqlite.connect(config.SQLITE_DB_PATH)
    try:
        # Get all queries with project context
        cursor = await conn.execute("""
            SELECT 
                sq.query_id,
                sq.query_text,
                sq.query_type,
                sq.created_at,
                sq.source_project_id,
                p.user_id,
                p.project_position,
                p.industry,
                p.skills,
                p.contribution,
                p.start_date,
                p.end_date
            FROM synthetic_queries sq
            LEFT JOIN projects p ON sq.source_project_id = p.user_project_history_id
            ORDER BY sq.query_type, sq.query_id
        """)
        
        rows = await cursor.fetchall()
        
        if not rows:
            print("No queries found in database.")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=[
            'query_id', 'query_text', 'query_type', 'created_at',
            'source_project_id', 'user_id', 'project_position', 
            'industry', 'skills', 'contribution', 'start_date', 'end_date'
        ])
        
        # Get query counts by type
        real_count = len(df[df['query_type'] == 'real'])
        specific_count = len(df[df['query_type'] == 'specific'])
        vague_count = len(df[df['query_type'] == 'vague'])
        
        print(f"Exporting {len(df)} queries:")
        print(f"  Real requirements: {real_count}")
        print(f"  Synthetic specific: {specific_count}")
        print(f"  Synthetic vague: {vague_count}")
        
        # Create Excel writer with multiple sheets
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # All queries
            df.to_excel(writer, sheet_name='All Queries', index=False)
            
            # Real requirements only
            if real_count > 0:
                df_real = df[df['query_type'] == 'real'][['query_id', 'query_text', 'created_at']]
                df_real.to_excel(writer, sheet_name='Real Requirements', index=False)
            
            # Synthetic queries with project context
            df_synthetic = df[df['query_type'].isin(['specific', 'vague'])]
            if len(df_synthetic) > 0:
                df_synthetic.to_excel(writer, sheet_name='Synthetic Queries', index=False)
            
            # Summary statistics
            summary_data = {
                'Metric': [
                    'Total Queries',
                    'Real Requirements',
                    'Synthetic Specific',
                    'Synthetic Vague',
                    'Unique Industries',
                    'Projects Sampled'
                ],
                'Count': [
                    len(df),
                    real_count,
                    specific_count,
                    vague_count,
                    df['industry'].nunique(),
                    df['source_project_id'].nunique() - (1 if real_count > 0 else 0)  # Exclude NULLs
                ]
            }
            df_summary = pd.DataFrame(summary_data)
            df_summary.to_excel(writer, sheet_name='Summary', index=False)
        
        print(f"\n✓ Exported to: {output_file}")
        
        # Show sample queries
        print("\nSample queries:")
        for query_type in ['real', 'specific', 'vague']:
            sample = df[df['query_type'] == query_type].head(3)
            if len(sample) > 0:
                print(f"\n{query_type.upper()}:")
                for _, row in sample.iterrows():
                    print(f"  - {row['query_text'][:100]}...")
    
    finally:
        await conn.close()


async def export_queries_by_industry(output_file: str = None):
    """
    Export queries grouped by industry for easier review.
    
    Args:
        output_file: Path to output Excel file (default: queries_by_industry_YYYYMMDD_HHMMSS.xlsx)
    """
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"queries_by_industry_{timestamp}.xlsx"
    
    conn = await aiosqlite.connect(config.SQLITE_DB_PATH)
    try:
        cursor = await conn.execute("""
            SELECT 
                sq.query_id,
                sq.query_text,
                sq.query_type,
                p.industry,
                p.project_position,
                p.skills,
                p.contribution
            FROM synthetic_queries sq
            LEFT JOIN projects p ON sq.source_project_id = p.user_project_history_id
            WHERE sq.query_type != 'real'  -- Only synthetic queries
            ORDER BY p.industry, sq.query_type, sq.query_id
        """)
        
        rows = await cursor.fetchall()
        
        if not rows:
            print("No synthetic queries found.")
            return
        
        df = pd.DataFrame(rows, columns=[
            'query_id', 'query_text', 'query_type', 'industry',
            'project_position', 'skills', 'contribution'
        ])
        
        print(f"Exporting {len(df)} synthetic queries by industry...")
        
        # Create Excel with one sheet per industry (top 10 industries)
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # All queries
            df.to_excel(writer, sheet_name='All', index=False)
            
            # Top industries
            top_industries = df['industry'].value_counts().head(10)
            for industry in top_industries.index:
                df_industry = df[df['industry'] == industry]
                sheet_name = industry[:31] if industry else 'Unknown'  # Excel sheet name limit
                df_industry.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"✓ Exported to: {output_file}")
    
    finally:
        await conn.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "by-industry":
        asyncio.run(export_queries_by_industry())
    else:
        asyncio.run(export_queries_to_excel())

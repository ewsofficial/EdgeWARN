#!/usr/bin/env python3
"""
profile_code.py - Profile Python code execution
Usage: python profile_code.py <script.py> [args...]
"""

import sys
import cProfile
import pstats
import io
from pstats import SortKey

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <script.py> [args...]")
        sys.exit(1)
    
    script = sys.argv[1]
    sys.argv = sys.argv[1:]  # Adjust argv for target script
    
    # Run profiler
    profiler = cProfile.Profile()
    
    try:
        with open(script, 'r') as f:
            code = compile(f.read(), script, 'exec')
        
        profiler.enable()
        exec(code, {'__name__': '__main__', '__file__': script})
        profiler.disable()
    except Exception as e:
        print(f"Error running script: {e}")
        sys.exit(1)
    
    # Generate report
    s = io.StringIO()
    stats = pstats.Stats(profiler, stream=s).sort_stats(SortKey.CUMULATIVE)
    
    print("\n" + "="*60)
    print("PERFORMANCE PROFILE")
    print("="*60 + "\n")
    
    print("## Top 20 Functions by Cumulative Time\n")
    stats.print_stats(20)
    print(s.getvalue())
    
    # Summary
    s2 = io.StringIO()
    stats2 = pstats.Stats(profiler, stream=s2).sort_stats(SortKey.TIME)
    print("\n## Top 10 Functions by Self Time\n")
    stats2.print_stats(10)
    print(s2.getvalue())

if __name__ == "__main__":
    main()

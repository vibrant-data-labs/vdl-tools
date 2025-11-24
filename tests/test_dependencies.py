"""
Smoke tests to verify core dependencies can be imported and work together.
"""
import sys


def test_core_imports():
    """Test that all core dependencies can be imported."""
    try:
        import numpy
        import pandas
        import networkx
        import sqlalchemy
        import requests
        import boto3
        import openai
        import torch
        import transformers
        import plotly
        import selenium
    except ImportError as e:
        raise AssertionError(f"Failed to import core dependency: {e}")


def test_version_compatibility():
    """Test that dependency versions are as expected."""
    import pandas as pd
    import numpy as np
    
    # Basic compatibility check - these should work together
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    arr = np.array([1, 2, 3])
    
    assert len(df) == 3
    assert len(arr) == 3
    assert df["a"].sum() == 6


def test_ml_stack():
    """Test that ML dependencies work together."""
    import torch
    import numpy as np
    
    # Create a simple tensor
    tensor = torch.tensor([1.0, 2.0, 3.0])
    numpy_array = tensor.numpy()
    
    assert len(numpy_array) == 3


def test_network_stack():
    """Test that network analysis dependencies work."""
    import networkx as nx
    import numpy as np
    
    # Create a simple graph
    G = nx.Graph()
    G.add_edge(1, 2)
    G.add_edge(2, 3)
    
    assert len(G.nodes()) == 3
    assert len(G.edges()) == 2


def test_data_processing_stack():
    """Test that data processing dependencies work together."""
    import pandas as pd
    import numpy as np
    from sklearn.preprocessing import StandardScaler
    
    # Create sample data
    data = pd.DataFrame({
        "feature1": [1, 2, 3, 4, 5],
        "feature2": [2, 4, 6, 8, 10]
    })
    
    # Scale the data
    scaler = StandardScaler()
    scaled = scaler.fit_transform(data)
    
    assert scaled.shape == (5, 2)


def test_python_version():
    """Verify Python version is 3.10+"""
    assert sys.version_info >= (3, 10), f"Python 3.10+ required, got {sys.version_info}"


if __name__ == "__main__":
    # Run tests manually
    test_core_imports()
    test_version_compatibility()
    test_ml_stack()
    test_network_stack()
    test_data_processing_stack()
    test_python_version()
    print("✅ All dependency checks passed!")


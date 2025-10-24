#!/bin/bash
# Script to configure Cursor to use Hatch environment

set -e

echo "🔍 Finding Hatch environment..."
HATCH_ENV_PATH=$(hatch env find default)
PYTHON_PATH="$HATCH_ENV_PATH/bin/python"

if [ ! -f "$PYTHON_PATH" ]; then
    echo "❌ Hatch environment not found. Creating it..."
    hatch env create
    HATCH_ENV_PATH=$(hatch env find default)
    PYTHON_PATH="$HATCH_ENV_PATH/bin/python"
fi

echo "✅ Found Python at: $PYTHON_PATH"
echo ""

# Create .vscode settings if it doesn't exist
mkdir -p .vscode

# Create or update settings.json
SETTINGS_FILE=".vscode/settings.json"

if [ -f "$SETTINGS_FILE" ]; then
    echo "📝 Updating existing .vscode/settings.json..."
    # Backup existing settings
    cp "$SETTINGS_FILE" "$SETTINGS_FILE.backup"
else
    echo "📝 Creating .vscode/settings.json..."
fi

# Write settings
cat > "$SETTINGS_FILE" << EOF
{
    "python.defaultInterpreterPath": "$PYTHON_PATH",
    "python.terminal.activateEnvironment": true,
    "python.analysis.extraPaths": [
        "\${workspaceFolder}/vdl_tools"
    ],
    "python.testing.pytestEnabled": true,
    "python.testing.unittestEnabled": false,
    "python.testing.pytestArgs": [
        "tests"
    ],
    "python.linting.enabled": true,
    "python.linting.ruffEnabled": true,
    "python.formatting.provider": "black",
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": {
        "source.organizeImports": "explicit"
    },
    "[python]": {
        "editor.defaultFormatter": "ms-python.black-formatter",
        "editor.formatOnSave": true
    }
}
EOF

echo "✅ Configuration saved to $SETTINGS_FILE"
echo ""
echo "Next steps:"
echo "  1. Reload Cursor window (Cmd+Shift+P → 'Developer: Reload Window')"
echo "  2. Or restart Cursor"
echo "  3. Check bottom-right corner for Python version"
echo ""
echo "Python path: $PYTHON_PATH"


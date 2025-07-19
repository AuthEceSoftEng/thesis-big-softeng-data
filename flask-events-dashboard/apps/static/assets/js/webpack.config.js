const path = require('path');

module.exports = {
    entry: './apps/static/assets/js/material-dashboard.min.js', // Your entry file
    output: {
        filename: 'bundle.js',
        path: path.resolve(__dirname, 'dist')
    },
    target: 'web', // Ensure the output is browser-compatible
};

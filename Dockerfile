FROM node@sha256:8e09ca51a4e5ba97e1f2f8e66b28bca613bf116e5be5dff6328b644d04bc4866

# Copy Source
COPY . /src

# Install Required Packages
WORKDIR /src
RUN npm install

# Run
CMD ["node", "./app.js"]
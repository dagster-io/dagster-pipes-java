package pipes.loaders;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import pipes.DagsterPipesException;
import pipes.data.PipesContextData;
import pipes.utils.PipesUtils;

import java.io.IOException;
import java.util.Map;

public class PipesS3ContextLoader extends PipesContextLoader {

    private final AmazonS3 client;

    public PipesS3ContextLoader(AmazonS3 client) {
        this.client = client;
    }

    @Override
    public PipesContextData loadContext(Map<String, Object> params) throws DagsterPipesException {
        String bucket = PipesUtils.assertEnvParamType(params, "bucket", String.class, this.getClass());
        String key = PipesUtils.assertEnvParamType(params, "key", String.class, this.getClass());
        S3Object s3Object = client.getObject(bucket, key);
        try (S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(inputStream, PipesContextData.class);
        } catch (IOException ioe) {
            throw new DagsterPipesException("Failed to load S3 object content!", ioe);
        }
    }

}

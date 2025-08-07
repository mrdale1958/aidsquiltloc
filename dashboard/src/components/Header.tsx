import React from 'react';
import {
  AppBar,
  Toolbar,
  Typography,
  Box,
  Chip,
  Container,
  Link,
  Stack,
} from '@mui/material';
import { styled } from '@mui/material/styles';
import AccountBalanceIcon from '@mui/icons-material/AccountBalance';
import FavoriteIcon from '@mui/icons-material/Favorite';
import DatasetIcon from '@mui/icons-material/Dataset';

const StyledAppBar = styled(AppBar)(({ theme }) => ({
  background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.primary.dark} 100%)`,
  boxShadow: '0 4px 20px rgba(0,0,0,0.1)',
}));

const BrandingBox = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(1),
  padding: theme.spacing(0.5, 1),
  borderRadius: theme.spacing(1),
  backgroundColor: 'rgba(255,255,255,0.1)',
  backdropFilter: 'blur(10px)',
}));

const PartnershipBox = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(2),
  [theme.breakpoints.down('md')]: {
    display: 'none',
  },
}));

const Header: React.FC = () => {
  return (
    <StyledAppBar position="static" elevation={0}>
      <Container maxWidth="xl">
        <Toolbar sx={{ py: 1 }}>
          {/* Main Title */}
          <Box sx={{ flexGrow: 1 }}>
            <Stack direction="row" alignItems="center" spacing={2}>
              <DatasetIcon sx={{ fontSize: 32, color: 'white' }} />
              <Box>
                <Typography
                  variant="h5"
                  component="h1"
                  sx={{
                    color: 'white',
                    fontWeight: 600,
                    lineHeight: 1.1,
                  }}
                >
                  AIDS Memorial Quilt Records
                </Typography>
                <Typography
                  variant="subtitle2"
                  sx={{
                    color: 'rgba(255,255,255,0.8)',
                    fontSize: '0.875rem',
                  }}
                >
                  Digital Archive & Research Dashboard
                </Typography>
              </Box>
            </Stack>
          </Box>

          {/* Partnership Branding */}
          <PartnershipBox>
            <Typography
              variant="body2"
              sx={{ color: 'rgba(255,255,255,0.7)', mr: 2 }}
            >
              In partnership with:
            </Typography>
            
            {/* Library of Congress Branding */}
            <BrandingBox>
              <AccountBalanceIcon sx={{ color: 'white', fontSize: 20 }} />
              <Box>
                <Link
                  href="https://www.loc.gov"
                  target="_blank"
                  rel="noopener noreferrer"
                  underline="none"
                  sx={{ color: 'white' }}
                >
                  <Typography variant="body2" sx={{ fontWeight: 600, fontSize: '0.8rem' }}>
                    Library of Congress
                  </Typography>
                </Link>
                <Typography variant="caption" sx={{ color: 'rgba(255,255,255,0.8)', display: 'block', lineHeight: 1 }}>
                  American Folklife Center
                </Typography>
              </Box>
            </BrandingBox>

            {/* AIDS Memorial Branding */}
            <BrandingBox>
              <FavoriteIcon sx={{ color: '#dc004e', fontSize: 20 }} />
              <Box>
                <Link
                  href="https://www.aidsmemorial.org"
                  target="_blank"
                  rel="noopener noreferrer"
                  underline="none"
                  sx={{ color: 'white' }}
                >
                  <Typography variant="body2" sx={{ fontWeight: 600, fontSize: '0.8rem' }}>
                    AIDS Memorial
                  </Typography>
                </Link>
                <Typography variant="caption" sx={{ color: 'rgba(255,255,255,0.8)', display: 'block', lineHeight: 1 }}>
                  National AIDS Memorial
                </Typography>
              </Box>
            </BrandingBox>
          </PartnershipBox>

          {/* Status Chip */}
          <Chip
            label="Live Data"
            size="small"
            sx={{
              backgroundColor: '#4caf50',
              color: 'white',
              fontWeight: 600,
              ml: 2,
              '& .MuiChip-label': {
                px: 1.5,
              },
            }}
          />
        </Toolbar>

        {/* Subtitle Bar */}
        <Box
          sx={{
            backgroundColor: 'rgba(255,255,255,0.1)',
            borderRadius: '8px 8px 0 0',
            px: 3,
            py: 1.5,
            mb: 0,
          }}
        >
          <Typography
            variant="body2"
            sx={{
              color: 'rgba(255,255,255,0.9)',
              textAlign: 'center',
              lineHeight: 1.4,
            }}
          >
            Preserving the memories and stories of those lost to AIDS through the world's largest 
            community-created memorial. Data sourced from the Library of Congress AIDS Memorial Quilt Records collection.
          </Typography>
        </Box>
      </Container>
    </StyledAppBar>
  );
};

export default Header;
